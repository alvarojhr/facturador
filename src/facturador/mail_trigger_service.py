import base64
from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import threading
from typing import Optional

from flask import Flask, abort, jsonify, request
from google.api_core.exceptions import NotFound
from google.cloud import firestore
from googleapiclient.errors import HttpError

from .mail_automation import (
    MailAutomationConfig,
    MailAutomationService,
    PollSummary,
    execute_google_with_retry,
    is_transient_google_error,
    load_mail_automation_config,
)


LOGGER = logging.getLogger(__name__)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value.strip())


def _str_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


class WatchStateStore:
    def __init__(self, project_id: str, collection_name: str, document_id: str) -> None:
        if project_id:
            self.client = firestore.Client(project=project_id)
        else:
            self.client = firestore.Client()
        self.doc_ref = self.client.collection(collection_name).document(document_id)

    def get_last_history_id(self) -> Optional[str]:
        snapshot = self.doc_ref.get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict() or {}
        value = data.get("last_history_id")
        return str(value) if value else None

    def set_last_history_id(self, history_id: str, watch_expiration: Optional[str] = None) -> None:
        payload = {
            "last_history_id": str(history_id),
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if watch_expiration:
            payload["watch_expiration"] = str(watch_expiration)
        self.doc_ref.set(payload, merge=True)

    def read_state(self) -> dict:
        snapshot = self.doc_ref.get()
        if not snapshot.exists:
            return {}
        return snapshot.to_dict() or {}


class LocalFileStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def get_last_history_id(self) -> Optional[str]:
        data = self.read_state()
        value = data.get("last_history_id")
        return str(value) if value else None

    def set_last_history_id(self, history_id: str, watch_expiration: Optional[str] = None) -> None:
        data = self.read_state()
        data["last_history_id"] = str(history_id)
        if watch_expiration is not None:
            data["watch_expiration"] = str(watch_expiration)
        self.path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def read_state(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}


class GmailPushProcessor:
    def __init__(self) -> None:
        config_path_raw = _str_env("FACTURADOR_AUTOMATION_CONFIG_PATH")
        config_path = Path(config_path_raw) if config_path_raw else None
        config = load_mail_automation_config(config_path)
        self.config = self._apply_env_overrides(config)
        self.mail = MailAutomationService(self.config)

        state_project = _str_env("FACTURADOR_STATE_PROJECT")
        state_collection = _str_env("FACTURADOR_STATE_COLLECTION", "facturador_state")
        state_doc = _str_env("FACTURADOR_STATE_DOC", "gmail_watch")
        force_local_state = _bool_env("FACTURADOR_FORCE_LOCAL_STATE", False)
        state_file = _str_env("FACTURADOR_STATE_FILE", "/tmp/facturador/gmail_watch_state.json")
        if force_local_state:
            self.state = LocalFileStateStore(Path(state_file))
        else:
            try:
                self.state = WatchStateStore(state_project, state_collection, state_doc)
            except Exception as exc:
                LOGGER.warning(
                    "No se pudo inicializar Firestore state store (%s). Usando estado local en archivo.", exc
                )
                self.state = LocalFileStateStore(Path(state_file))

        self.gmail_user = _str_env("FACTURADOR_GMAIL_USER", "me")
        self.max_sync_cycles = _int_env("FACTURADOR_SYNC_MAX_CYCLES", 20)
        self.watch_topic = _str_env("FACTURADOR_WATCH_TOPIC")
        labels_raw = _str_env("FACTURADOR_WATCH_LABEL_IDS", "INBOX")
        self.watch_label_ids = [item.strip() for item in labels_raw.split(",") if item.strip()]
        self.watch_label_filter_action = _str_env("FACTURADOR_WATCH_LABEL_FILTER_ACTION", "include")
        self.watch_sync_after_start = _bool_env("FACTURADOR_WATCH_SYNC_AFTER_START", True)

    def _apply_env_overrides(self, config: MailAutomationConfig) -> MailAutomationConfig:
        credentials_path = _str_env("FACTURADOR_CREDENTIALS_PATH")
        token_path = _str_env("FACTURADOR_TOKEN_PATH")
        drive_folder_id = _str_env("FACTURADOR_DRIVE_PARENT_FOLDER_ID")
        local_work_dir = _str_env("FACTURADOR_LOCAL_WORK_DIR")
        rules_path = _str_env("FACTURADOR_RULES_PATH")

        if credentials_path:
            config.credentials_path = Path(credentials_path)
        if token_path:
            config.token_path = Path(token_path)
        if drive_folder_id:
            config.drive_parent_folder_id = drive_folder_id
        if local_work_dir:
            config.local_work_dir = Path(local_work_dir)
        if rules_path:
            config.rules_path = Path(rules_path)

        max_per_poll = _str_env("FACTURADOR_MAX_MESSAGES_PER_POLL")
        if max_per_poll:
            config.max_messages_per_poll = int(max_per_poll)

        return config

    def start_watch(self) -> dict:
        if not self.watch_topic:
            raise ValueError("Configura FACTURADOR_WATCH_TOPIC para iniciar watch.")

        body: dict = {"topicName": self.watch_topic}
        if self.watch_label_ids:
            body["labelIds"] = self.watch_label_ids
            body["labelFilterAction"] = self.watch_label_filter_action

        response = execute_google_with_retry(
            lambda: self.mail.gmail.users().watch(userId=self.gmail_user, body=body).execute(),
            operation="gmail.users.watch",
        )
        history_id = str(response.get("historyId") or "")
        if history_id:
            self.state.set_last_history_id(history_id, watch_expiration=str(response.get("expiration") or ""))

        sync = None
        if self.watch_sync_after_start:
            summary = self.mail.drain_unprocessed_messages(max_cycles=self.max_sync_cycles)
            sync = asdict(summary)

        return {
            "watch": response,
            "sync": sync,
        }

    def process_push_history(self, new_history_id: str) -> dict:
        current_history_id = self.state.get_last_history_id()

        if not current_history_id:
            summary = self.mail.drain_unprocessed_messages(max_cycles=self.max_sync_cycles)
            self.state.set_last_history_id(new_history_id)
            return {
                "mode": "bootstrap_sync",
                "new_history_id": new_history_id,
                "summary": asdict(summary),
            }

        try:
            message_ids = self._list_message_ids_from_history(current_history_id)
        except HttpError as exc:
            if getattr(exc, "resp", None) is not None and exc.resp.status == 404:
                summary = self.mail.drain_unprocessed_messages(max_cycles=self.max_sync_cycles)
                self.state.set_last_history_id(new_history_id)
                return {
                    "mode": "history_gap_full_sync",
                    "new_history_id": new_history_id,
                    "summary": asdict(summary),
                }
            raise

        processed = PollSummary()
        for message_id in message_ids:
            try:
                ok_count, skip_count, failed = self.mail.process_message_by_id(message_id)
            except Exception as exc:
                if is_transient_google_error(exc):
                    raise
                LOGGER.exception("Fallo procesando message_id=%s desde history incremental: %s", message_id, exc)
                processed.checked_messages += 1
                processed.failed_messages += 1
                continue
            processed.checked_messages += 1
            processed.processed_attachments += ok_count
            processed.skipped_messages += skip_count
            if failed:
                processed.failed_messages += 1
            else:
                processed.processed_messages += 1

        self.state.set_last_history_id(new_history_id)
        return {
            "mode": "history_incremental",
            "new_history_id": new_history_id,
            "message_ids": len(message_ids),
            "summary": asdict(processed),
        }

    def _list_message_ids_from_history(self, start_history_id: str) -> list[str]:
        ids: dict[str, bool] = {}
        page_token = None
        while True:
            history_req = self.mail.gmail.users().history().list(
                userId=self.gmail_user,
                startHistoryId=str(start_history_id),
                historyTypes=["messageAdded"],
                pageToken=page_token,
                maxResults=500,
            )
            response = execute_google_with_retry(
                lambda: history_req.execute(),
                operation="gmail.users.history.list",
            )

            for item in response.get("history", []):
                for added in item.get("messagesAdded", []):
                    msg = added.get("message") or {}
                    msg_id = str(msg.get("id") or "").strip()
                    if msg_id:
                        ids[msg_id] = True

            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return list(ids.keys())

    def manual_sync(self, max_cycles: Optional[int] = None) -> PollSummary:
        return self.mail.drain_unprocessed_messages(max_cycles=max_cycles or self.max_sync_cycles)

    def read_state(self) -> dict:
        return self.state.read_state()


def _require_admin_token() -> None:
    token = _str_env("FACTURADOR_ADMIN_TOKEN")
    if not token:
        return

    provided = request.headers.get("X-Facturador-Admin-Token", "")
    if provided != token:
        abort(401, description="Unauthorized")


def _decode_pubsub_payload(request_body: dict) -> dict:
    if not request_body or "message" not in request_body:
        raise ValueError("Payload Pub/Sub invalido: falta 'message'.")
    message = request_body.get("message") or {}
    data_b64 = message.get("data")
    if not data_b64:
        raise ValueError("Payload Pub/Sub invalido: falta 'message.data'.")
    raw = base64.b64decode(data_b64)
    decoded = raw.decode("utf-8")
    payload = json.loads(decoded)
    if not isinstance(payload, dict):
        raise ValueError("Payload Pub/Sub invalido: JSON de Gmail no es objeto.")
    return payload


def create_app() -> Flask:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    app = Flask(__name__)
    processor = GmailPushProcessor()
    operation_lock = threading.Lock()

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True}), 200

    @app.post("/pubsub/push")
    def pubsub_push():
        body = request.get_json(silent=True) or {}
        try:
            payload = _decode_pubsub_payload(body)
            history_id = str(payload.get("historyId") or "").strip()
            if not history_id:
                raise ValueError("Notificacion Gmail sin historyId.")
            if not operation_lock.acquire(blocking=False):
                LOGGER.info("Push omitido por operacion en curso. history_id=%s", history_id)
                return jsonify({"ok": True, "skipped": "busy"}), 200
            try:
                result = processor.process_push_history(history_id)
                LOGGER.info("Push procesado: %s", result)
            finally:
                operation_lock.release()
        except Exception as exc:
            if is_transient_google_error(exc):
                LOGGER.warning("Error transitorio procesando push. Se confirma para evitar tormenta de reintentos: %s", exc)
                return jsonify({"ok": True, "retryable_error": str(exc)}), 200
            LOGGER.exception("Error procesando push: %s", exc)
            return jsonify({"ok": True, "error": str(exc)}), 200
        return jsonify({"ok": True}), 200

    @app.post("/admin/start-watch")
    def start_watch():
        _require_admin_token()
        if not operation_lock.acquire(blocking=False):
            return jsonify({"ok": True, "skipped": "busy"}), 200
        try:
            try:
                result = processor.start_watch()
            except Exception as exc:
                if is_transient_google_error(exc):
                    LOGGER.warning("Error transitorio iniciando watch: %s", exc)
                    return jsonify({"ok": False, "retryable": True, "error": str(exc)}), 200
                LOGGER.exception("Error iniciando watch: %s", exc)
                return jsonify({"ok": False, "error": str(exc)}), 500
        finally:
            operation_lock.release()
        return jsonify(result), 200

    @app.post("/admin/full-sync")
    def full_sync():
        _require_admin_token()
        max_cycles = request.args.get("max_cycles", default=None, type=int)
        if not operation_lock.acquire(blocking=False):
            return jsonify({"ok": True, "skipped": "busy"}), 200
        try:
            try:
                summary = processor.manual_sync(max_cycles=max_cycles)
            except Exception as exc:
                if is_transient_google_error(exc):
                    LOGGER.warning("Error transitorio en full-sync: %s", exc)
                    return jsonify({"ok": False, "retryable": True, "error": str(exc)}), 200
                LOGGER.exception("Error en full-sync: %s", exc)
                return jsonify({"ok": False, "error": str(exc)}), 500
        finally:
            operation_lock.release()
        return jsonify(asdict(summary)), 200

    @app.get("/admin/state")
    def state():
        _require_admin_token()
        data = processor.read_state()
        return jsonify(data), 200

    return app


app = create_app()
