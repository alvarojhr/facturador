import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal
import io
import json
import logging
import os
from pathlib import Path
import socket
import ssl
import sys
import threading
import time
from typing import Optional
import urllib.error
import urllib.request
import zipfile

from googleapiclient.errors import HttpError

from .invoice_parser import extract_invoice_root_from_bytes, parse_invoice_header
from .pricing import MarkupConfig
from .processor import ProcessResult, process_invoice_bytes


LOGGER = logging.getLogger(__name__)

GMAIL_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive",
]


class AutomationError(Exception):
    pass


class OAuthTokenInvalidError(AutomationError):
    def __init__(self, message: str, reason: str = "oauth_invalid_grant") -> None:
        super().__init__(message)
        self.reason = reason


@dataclass
class MailAutomationConfig:
    gmail_query: str = "has:attachment filename:zip in:inbox"
    processed_label_name: str = "facturador-procesado"
    mark_as_read: bool = True
    poll_interval_sec: int = 60
    max_messages_per_poll: Optional[int] = 20
    drive_parent_folder_id: str = ""
    credentials_path: Path = Path("config/google_credentials.json")
    token_path: Path = Path("config/google_token.json")
    token_store_project: str = ""
    token_store_collection: str = ""
    token_store_doc: str = "gmail_oauth_token"
    local_work_dir: Path = Path("automation_work")
    rules_path: Optional[Path] = None
    sheet_name: str = "Productos"
    entered_label_name: str = "Ingresado"
    entered_synced_label_name: str = "facturador-drive-ingresado"
    entered_drive_subfolder_name: str = "Ingresado"
    sync_entered_label: bool = True
    markup_threshold: Decimal = Decimal("10000")
    markup_below: Decimal = Decimal("0.68")
    markup_above: Decimal = Decimal("1.32")
    round_net_step: Decimal = Decimal("100")
    rounding_mode: str = "up"
    erp_base_url: str = ""
    erp_api_key: str = ""
    artifacts_bucket_name: str = ""
    artifacts_prefix: str = "facturador-artifacts"

    def pricing_config(self) -> MarkupConfig:
        return MarkupConfig(
            threshold=self.markup_threshold,
            below_divisor=self.markup_below,
            above_multiplier=self.markup_above,
            round_net_step=self.round_net_step,
            rounding_mode=self.rounding_mode,
        )


@dataclass
class PollSummary:
    checked_messages: int = 0
    processed_messages: int = 0
    processed_attachments: int = 0
    failed_messages: int = 0
    skipped_messages: int = 0
    bytes_processed: int = 0
    gmail_list_ms: float = 0.0
    gmail_download_ms: float = 0.0
    parse_ms: float = 0.0
    pricing_ms: float = 0.0
    erp_ms: float = 0.0
    label_ms: float = 0.0
    drive_ms: float = 0.0
    gcs_ms: float = 0.0
    artifact_ms: float = 0.0

    def merge(self, other: "PollSummary") -> None:
        self.checked_messages += other.checked_messages
        self.processed_messages += other.processed_messages
        self.processed_attachments += other.processed_attachments
        self.failed_messages += other.failed_messages
        self.skipped_messages += other.skipped_messages
        self.bytes_processed += other.bytes_processed
        self.gmail_list_ms += other.gmail_list_ms
        self.gmail_download_ms += other.gmail_download_ms
        self.parse_ms += other.parse_ms
        self.pricing_ms += other.pricing_ms
        self.erp_ms += other.erp_ms
        self.label_ms += other.label_ms
        self.drive_ms += other.drive_ms
        self.gcs_ms += other.gcs_ms
        self.artifact_ms += other.artifact_ms


@dataclass(frozen=True)
class RuntimeOptions:
    skip_drive: bool = False
    skip_ingresado_sync: bool = False
    concurrency: int = 1


@dataclass
class DownloadedMessage:
    message_id: str
    subject: str
    attachments: list[tuple[str, bytes]]
    download_ms: float
    bytes_processed: int


@dataclass
class AttachmentSyncMetrics:
    drive_ms: float = 0.0
    erp_ms: float = 0.0
    gcs_ms: float = 0.0


@dataclass
class MessageProcessingOutcome:
    message_id: str
    should_mark_processed: bool
    summary: PollSummary = field(default_factory=PollSummary)


class FirestoreOAuthTokenStore:
    def __init__(self, project_id: str, collection_name: str, document_id: str) -> None:
        firestore = _import_google_firestore_dep()
        self._firestore = firestore
        if project_id:
            self.client = firestore.Client(project=project_id)
        else:
            self.client = firestore.Client()
        self.doc_ref = self.client.collection(collection_name).document(document_id)

    def load_token_json(self) -> Optional[str]:
        snapshot = self.doc_ref.get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict() or {}
        token_json = data.get("token_json")
        if not token_json:
            return None
        return str(token_json)

    def save_token_json(self, token_json: str, source: str) -> None:
        self.doc_ref.set(
            {
                "token_json": token_json,
                "source": source,
                "updated_at": self._firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )


def _app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def _resolve_optional_path(base: Path, raw_value: Optional[str]) -> Optional[Path]:
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    return base / path


def default_mail_automation_config_path() -> Path:
    return _app_base_dir() / "config" / "mail_automation.json"


def _parse_optional_positive_int(value, field_name: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        value = text

    parsed = int(value)
    if parsed < 1:
        raise AutomationError(f"{field_name} debe ser >= 1.")
    return parsed


def load_mail_automation_config(path: Optional[Path] = None) -> MailAutomationConfig:
    cfg_path = path or default_mail_automation_config_path()
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"No se encontro configuracion de automatizacion en: {cfg_path}\n"
            "Crea config/mail_automation.json basado en config/mail_automation.example.json."
        )

    try:
        payload = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise AutomationError(f"No se pudo leer {cfg_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise AutomationError("mail_automation.json invalido: se esperaba un objeto JSON.")

    base = _app_base_dir()
    cfg = MailAutomationConfig()

    cfg.gmail_query = str(payload.get("gmail_query", cfg.gmail_query)).strip() or cfg.gmail_query
    cfg.processed_label_name = (
        str(payload.get("processed_label_name", cfg.processed_label_name)).strip() or cfg.processed_label_name
    )
    cfg.mark_as_read = bool(payload.get("mark_as_read", cfg.mark_as_read))
    cfg.poll_interval_sec = int(payload.get("poll_interval_sec", cfg.poll_interval_sec))
    cfg.max_messages_per_poll = _parse_optional_positive_int(
        payload.get("max_messages_per_poll", cfg.max_messages_per_poll),
        "max_messages_per_poll",
    )
    cfg.drive_parent_folder_id = str(payload.get("drive_parent_folder_id", "")).strip()

    credentials = _resolve_optional_path(base, payload.get("credentials_path"))
    token = _resolve_optional_path(base, payload.get("token_path"))
    local_dir = _resolve_optional_path(base, payload.get("local_work_dir"))
    rules = _resolve_optional_path(base, payload.get("rules_path"))

    cfg.credentials_path = credentials or (base / "config" / "google_credentials.json")
    cfg.token_path = token or (base / "config" / "google_token.json")
    cfg.token_store_project = str(payload.get("token_store_project", cfg.token_store_project)).strip()
    cfg.token_store_collection = str(payload.get("token_store_collection", cfg.token_store_collection)).strip()
    cfg.token_store_doc = str(payload.get("token_store_doc", cfg.token_store_doc)).strip() or cfg.token_store_doc
    cfg.local_work_dir = local_dir or (base / "automation_work")
    cfg.rules_path = rules

    cfg.sheet_name = str(payload.get("sheet_name", cfg.sheet_name)).strip() or cfg.sheet_name
    cfg.entered_label_name = str(payload.get("entered_label_name", cfg.entered_label_name)).strip()
    cfg.entered_synced_label_name = str(
        payload.get("entered_synced_label_name", cfg.entered_synced_label_name)
    ).strip()
    cfg.entered_drive_subfolder_name = str(
        payload.get("entered_drive_subfolder_name", cfg.entered_drive_subfolder_name)
    ).strip()
    cfg.sync_entered_label = bool(payload.get("sync_entered_label", cfg.sync_entered_label))
    cfg.markup_threshold = Decimal(str(payload.get("markup_threshold", cfg.markup_threshold)))
    cfg.markup_below = Decimal(str(payload.get("markup_below", cfg.markup_below)))
    cfg.markup_above = Decimal(str(payload.get("markup_above", cfg.markup_above)))
    cfg.round_net_step = Decimal(str(payload.get("round_net_step", cfg.round_net_step)))
    cfg.rounding_mode = str(payload.get("rounding_mode", cfg.rounding_mode)).strip() or cfg.rounding_mode
    cfg.erp_base_url = str(payload.get("erp_base_url", cfg.erp_base_url)).strip()
    cfg.erp_api_key = str(payload.get("erp_api_key", cfg.erp_api_key)).strip()
    cfg.artifacts_bucket_name = str(payload.get("artifacts_bucket_name", cfg.artifacts_bucket_name)).strip()
    cfg.artifacts_prefix = str(payload.get("artifacts_prefix", cfg.artifacts_prefix)).strip() or cfg.artifacts_prefix

    if cfg.poll_interval_sec < 10:
        raise AutomationError("poll_interval_sec debe ser >= 10 segundos.")
    if cfg.sync_entered_label:
        if not cfg.entered_label_name:
            raise AutomationError("entered_label_name no puede ser vacio cuando sync_entered_label=true.")
        if not cfg.entered_synced_label_name:
            raise AutomationError("entered_synced_label_name no puede ser vacio cuando sync_entered_label=true.")
        if not cfg.entered_drive_subfolder_name:
            raise AutomationError("entered_drive_subfolder_name no puede ser vacio cuando sync_entered_label=true.")
    if cfg.rounding_mode not in {"up", "down", "nearest"}:
        raise AutomationError("rounding_mode invalido. Debe ser: up, down o nearest.")
    if cfg.token_store_collection and not cfg.token_store_doc:
        raise AutomationError("token_store_doc no puede ser vacio cuando token_store_collection esta configurado.")

    return cfg


def _import_google_deps():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError as exc:
        raise AutomationError(
            "Faltan dependencias de Google.\n"
            "Ejecuta: pip install -r requirements.txt"
        ) from exc
    return Request, Credentials, InstalledAppFlow, build, MediaFileUpload


def _import_google_storage_dep():
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise AutomationError(
            "Falta dependencia de Google Cloud Storage.\n"
            "Ejecuta: pip install -r requirements.txt"
        ) from exc
    return storage


def _import_google_firestore_dep():
    try:
        from google.cloud import firestore
    except ImportError as exc:
        raise AutomationError(
            "Falta dependencia de Google Cloud Firestore.\n"
            "Ejecuta: pip install -r requirements.txt"
        ) from exc
    return firestore


def _safe_name(value: str) -> str:
    invalid = '<>:"/\\\\|?*'
    cleaned = "".join("_" if ch in invalid or ord(ch) < 32 else ch for ch in value)
    cleaned = cleaned.strip().strip(".")
    return cleaned or "archivo"


def _message_subject(message_payload: dict) -> str:
    headers = (message_payload.get("payload") or {}).get("headers") or []
    for header in headers:
        if str(header.get("name", "")).lower() == "subject":
            return str(header.get("value", "")).strip()
    return ""


def _gmail_label_query(label_name: str) -> str:
    escaped = label_name.replace('"', '\\"')
    return f'label:"{escaped}"'


def _iter_parts(payload: dict):
    stack = [payload]
    while stack:
        part = stack.pop()
        yield part
        child_parts = part.get("parts") or []
        for child in child_parts:
            stack.append(child)


def _escape_drive_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _is_skippable_attachment_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = [
        "no se encontro un xml de invoice o creditnote embebido",
        "el documento no contiene lineas de items",
    ]
    return any(marker in text for marker in markers)


_TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}


def _http_error_status(exc: Exception) -> Optional[int]:
    if not isinstance(exc, HttpError):
        return None
    resp = getattr(exc, "resp", None)
    if resp is None:
        return None
    try:
        return int(getattr(resp, "status", None))
    except (TypeError, ValueError):
        return None


def _is_transient_google_error(exc: Exception) -> bool:
    status = _http_error_status(exc)
    if status is not None:
        return status in _TRANSIENT_HTTP_STATUS

    if isinstance(exc, (TimeoutError, socket.timeout, ssl.SSLError)):
        return True

    if isinstance(exc, OSError):
        message = str(exc).lower()
        return "timed out" in message or "temporarily unavailable" in message

    return False


def is_transient_google_error(exc: Exception) -> bool:
    return _is_transient_google_error(exc)


def _is_oauth_invalid_grant(exc: Exception) -> bool:
    text = str(exc).lower()
    if "invalid_grant" in text:
        return True
    return "token has been expired or revoked" in text


def _is_cloud_runtime() -> bool:
    return bool(os.getenv("K_SERVICE"))


def execute_google_with_retry(action, operation: str, attempts: int = 4, base_delay_sec: float = 1.0):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if not _is_transient_google_error(exc) or attempt >= attempts:
                raise
            delay = min(base_delay_sec * (2 ** (attempt - 1)), 8.0)
            LOGGER.warning(
                "Error transitorio Google API en %s (intento %s/%s): %s. Reintentando en %.1fs.",
                operation,
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    if last_error is not None:
        raise last_error


class MailAutomationService:
    def __init__(self, config: MailAutomationConfig) -> None:
        self.config = config
        self.config.local_work_dir.mkdir(parents=True, exist_ok=True)
        (self.config.local_work_dir / "incoming").mkdir(parents=True, exist_ok=True)
        (self.config.local_work_dir / "output").mkdir(parents=True, exist_ok=True)
        self._token_store = self._build_token_store()
        self.gmail, self.drive, self.google_credentials = self._build_google_services()
        self._storage_client = None
        self._drive_lock = threading.RLock()
        self._drive_folder_cache: dict[tuple[str, str], str] = {}
        self._drive_folder_files_cache: dict[str, set[str]] = {}
        self.processed_label_id = self._ensure_gmail_label(self.config.processed_label_name)
        self.entered_label_id: Optional[str] = None
        self.entered_synced_label_id: Optional[str] = None
        if self.config.sync_entered_label:
            self.entered_label_id = self._ensure_gmail_label(self.config.entered_label_name)
            self.entered_synced_label_id = self._ensure_gmail_label(self.config.entered_synced_label_name)

    def _build_google_services(self):
        Request, Credentials, InstalledAppFlow, build, _ = _import_google_deps()

        creds = self._load_credentials_from_token_store(Credentials)
        loaded_from = "token_store" if creds is not None else ""
        force_interactive_reauth = False
        if creds is None and self.config.token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(self.config.token_path), GMAIL_DRIVE_SCOPES)
                loaded_from = "token_file"
            except Exception as exc:
                LOGGER.warning("No se pudo leer token OAuth desde %s (%s).", self.config.token_path, exc)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    self._persist_oauth_credentials(creds, source="refresh")
                except Exception as exc:
                    if _is_oauth_invalid_grant(exc):
                        if _is_cloud_runtime():
                            raise OAuthTokenInvalidError(
                                "Token OAuth de Google expirado o revocado (invalid_grant). "
                                "Reautentica la cuenta y actualiza el token bootstrap para reactivar el servicio.",
                            ) from exc
                        LOGGER.warning(
                            "Token OAuth invalido (invalid_grant). Se iniciara reautenticacion interactiva local."
                        )
                        force_interactive_reauth = True
                        creds = None
                    else:
                        raise
            if force_interactive_reauth or not creds or not creds.valid:
                if not self.config.credentials_path.exists():
                    raise FileNotFoundError(
                        "No se encontro credentials de Google OAuth.\n"
                        f"Ruta esperada: {self.config.credentials_path}"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(str(self.config.credentials_path), GMAIL_DRIVE_SCOPES)
                creds = flow.run_local_server(port=0)
                self._persist_oauth_credentials(creds, source="interactive")
        elif loaded_from == "token_file":
            # Backfill bootstrap token into Firestore so refreshes do not depend on a read-only secret mount.
            self._persist_oauth_credentials(creds, source="bootstrap_file")

        gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        return gmail, drive, creds

    def _build_token_store(self) -> Optional[FirestoreOAuthTokenStore]:
        if not self.config.token_store_collection:
            return None
        try:
            return FirestoreOAuthTokenStore(
                project_id=self.config.token_store_project,
                collection_name=self.config.token_store_collection,
                document_id=self.config.token_store_doc,
            )
        except Exception as exc:
            LOGGER.warning("No se pudo inicializar token store OAuth en Firestore (%s).", exc)
            return None

    def _load_credentials_from_token_store(self, Credentials):
        if self._token_store is None:
            return None

        try:
            token_json = self._token_store.load_token_json()
        except Exception as exc:
            LOGGER.warning("No se pudo leer token OAuth desde Firestore (%s).", exc)
            return None

        if not token_json:
            return None

        try:
            payload = json.loads(token_json)
            if not isinstance(payload, dict):
                raise ValueError("token_json no es un objeto JSON")
            return Credentials.from_authorized_user_info(payload, GMAIL_DRIVE_SCOPES)
        except Exception as exc:
            LOGGER.warning("Token OAuth almacenado en Firestore es invalido (%s).", exc)
            return None

    def _persist_oauth_credentials(self, creds, source: str) -> None:
        token_json = creds.to_json()
        stored_in_firestore = False

        if self._token_store is not None:
            try:
                self._token_store.save_token_json(token_json, source=source)
                stored_in_firestore = True
            except Exception as exc:
                LOGGER.warning("No se pudo persistir token OAuth en Firestore (%s).", exc)

        if stored_in_firestore and self._is_secret_mount_path(self.config.token_path):
            return

        try:
            self.config.token_path.parent.mkdir(parents=True, exist_ok=True)
            self.config.token_path.write_text(token_json, encoding="utf-8")
        except OSError as exc:
            level = logging.INFO if stored_in_firestore else logging.WARNING
            LOGGER.log(
                level,
                "No se pudo persistir token OAuth en %s (%s).%s",
                self.config.token_path,
                exc,
                " Firestore conserva el token vigente." if stored_in_firestore else " Se continua con credenciales en memoria.",
            )

    def _is_secret_mount_path(self, path: Path) -> bool:
        normalized = str(path).replace("\\", "/")
        return normalized.startswith("/secrets/")

    def _ensure_gmail_label(self, label_name: str) -> str:
        labels_resp = execute_google_with_retry(
            lambda: self.gmail.users().labels().list(userId="me").execute(),
            operation="gmail.labels.list",
        )
        labels = labels_resp.get("labels", [])
        for label in labels:
            if label.get("name") == label_name:
                return str(label["id"])

        created = execute_google_with_retry(
            lambda: self.gmail.users().labels().create(
                userId="me",
                body={
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            ).execute(),
            operation="gmail.labels.create",
        )
        return str(created["id"])

    def _resolve_runtime_options(self, runtime: Optional[RuntimeOptions] = None) -> RuntimeOptions:
        if runtime is None:
            return RuntimeOptions()

        concurrency = runtime.concurrency if runtime.concurrency > 0 else 1
        skip_drive = runtime.skip_drive
        skip_ingresado_sync = runtime.skip_ingresado_sync or skip_drive
        return RuntimeOptions(
            skip_drive=skip_drive,
            skip_ingresado_sync=skip_ingresado_sync,
            concurrency=concurrency,
        )

    def _validate_runtime_options(self, runtime: RuntimeOptions) -> None:
        if runtime.skip_drive and not self.config.erp_base_url:
            raise AutomationError("skip_drive requiere erp_base_url configurado.")
        if not runtime.skip_drive and not self.config.drive_parent_folder_id:
            raise AutomationError("Configura drive_parent_folder_id para ejecutar con Google Drive.")
        if not runtime.skip_ingresado_sync and self.config.sync_entered_label and not self.config.drive_parent_folder_id:
            raise AutomationError("Configura drive_parent_folder_id para sincronizar el label 'Ingresado'.")

    def run_forever(self) -> None:
        LOGGER.info("Automatizacion Gmail/Drive iniciada. Intervalo: %ss", self.config.poll_interval_sec)
        while True:
            try:
                summary = self.run_once(runtime=RuntimeOptions())
                LOGGER.info(
                    "Ciclo completado. mensajes=%s procesados=%s adjuntos=%s fallidos=%s bytes=%s",
                    summary.checked_messages,
                    summary.processed_messages,
                    summary.processed_attachments,
                    summary.failed_messages,
                    summary.bytes_processed,
                )
            except Exception as exc:
                LOGGER.exception("Fallo en ciclo de automatizacion: %s", exc)
            time.sleep(self.config.poll_interval_sec)

    def run_once(self, runtime: Optional[RuntimeOptions] = None) -> PollSummary:
        runtime_options = self._resolve_runtime_options(runtime)
        self._validate_runtime_options(runtime_options)

        summary = PollSummary()
        list_started = time.perf_counter()
        messages = self.query_unprocessed_messages(limit=self.config.max_messages_per_poll)
        summary.gmail_list_ms = (time.perf_counter() - list_started) * 1000
        summary.checked_messages = len(messages)

        if not messages:
            if self.config.sync_entered_label and not runtime_options.skip_ingresado_sync:
                self.sync_ingresado_messages(limit=self.config.max_messages_per_poll)
            return summary

        if runtime_options.concurrency <= 1 or len(messages) <= 1:
            for msg_ref in messages:
                msg_id = str(msg_ref.get("id"))
                if not msg_id:
                    continue
                message = self._download_message(message_id=msg_id, operation="gmail.messages.get")
                outcome = self._process_downloaded_message(message, runtime_options)
                summary.merge(outcome.summary)
                if outcome.should_mark_processed:
                    summary.label_ms += self._mark_message_processed(message.message_id)
        else:
            downloaded_messages: list[DownloadedMessage] = []
            for msg_ref in messages:
                msg_id = str(msg_ref.get("id"))
                if not msg_id:
                    continue
                downloaded_messages.append(self._download_message(message_id=msg_id, operation="gmail.messages.get"))

            with ThreadPoolExecutor(max_workers=min(runtime_options.concurrency, len(downloaded_messages))) as executor:
                futures = {
                    executor.submit(self._process_downloaded_message, message, runtime_options): message.message_id
                    for message in downloaded_messages
                }
                for future in as_completed(futures):
                    outcome = future.result()
                    summary.merge(outcome.summary)
                    if outcome.should_mark_processed:
                        summary.label_ms += self._mark_message_processed(outcome.message_id)

        if self.config.sync_entered_label and not runtime_options.skip_ingresado_sync:
            self.sync_ingresado_messages(limit=self.config.max_messages_per_poll)

        return summary

    def query_unprocessed_messages(self, limit: Optional[int] = None) -> list[dict]:
        processed_query = _gmail_label_query(self.config.processed_label_name)
        query = f"({self.config.gmail_query}) -{processed_query}"
        return self._list_messages(query=query, limit=limit, operation="gmail.messages.list")

    def query_ingresado_pending_messages(self, limit: Optional[int] = None) -> list[dict]:
        if not self.config.sync_entered_label:
            return []

        query = (
            f"{_gmail_label_query(self.config.entered_label_name)} "
            f"{_gmail_label_query(self.config.processed_label_name)} "
            f"-{_gmail_label_query(self.config.entered_synced_label_name)} "
            "has:attachment filename:zip"
        )
        return self._list_messages(query=query, limit=limit, operation="gmail.messages.list.ingresado")

    def sync_ingresado_messages(self, limit: Optional[int] = None) -> int:
        if not self.config.sync_entered_label:
            return 0

        pending = self.query_ingresado_pending_messages(limit=limit)
        moved_folders = 0
        for msg_ref in pending:
            message_id = str(msg_ref.get("id") or "").strip()
            if not message_id:
                continue

            try:
                moved_folders += self._sync_ingresado_message(message_id)
                self._mark_ingresado_synced(message_id)
            except Exception as exc:
                LOGGER.exception("No se pudo sincronizar mensaje 'Ingresado' id=%s: %s", message_id, exc)

        if pending:
            LOGGER.info(
                "Sincronizacion 'Ingresado' completada. mensajes=%s carpetas_movidas=%s",
                len(pending),
                moved_folders,
            )
        return moved_folders

    def process_message_by_id(self, message_id: str, runtime: Optional[RuntimeOptions] = None) -> tuple[int, int, bool]:
        runtime_options = self._resolve_runtime_options(runtime)
        self._validate_runtime_options(runtime_options)
        message = self._download_message(message_id=message_id, operation="gmail.messages.get.by_id")
        outcome = self._process_downloaded_message(message, runtime_options)
        if outcome.should_mark_processed:
            outcome.summary.label_ms += self._mark_message_processed(message_id)
        return (
            outcome.summary.processed_attachments,
            outcome.summary.skipped_messages,
            outcome.summary.failed_messages > 0,
        )

    def drain_unprocessed_messages(
        self,
        max_cycles: int = 20,
        runtime: Optional[RuntimeOptions] = None,
    ) -> PollSummary:
        merged = PollSummary()
        for _ in range(max_cycles):
            cycle = self.run_once(runtime=runtime)
            merged.merge(cycle)
            if cycle.checked_messages == 0:
                break
        return merged

    def _list_messages(self, query: str, operation: str, limit: Optional[int] = None) -> list[dict]:
        resolved_limit = self.config.max_messages_per_poll if limit is None else limit
        messages: list[dict] = []
        page_token: Optional[str] = None
        remaining = resolved_limit

        while True:
            request_kwargs = {
                "userId": "me",
                "q": query,
            }
            if page_token:
                request_kwargs["pageToken"] = page_token
            if remaining is None:
                request_kwargs["maxResults"] = 500
            else:
                request_kwargs["maxResults"] = min(remaining, 500)

            listed = execute_google_with_retry(
                lambda request_kwargs=request_kwargs: self.gmail.users().messages().list(**request_kwargs).execute(),
                operation=operation,
            )
            batch = listed.get("messages", [])
            messages.extend(batch)

            if remaining is not None:
                remaining -= len(batch)
                if remaining <= 0:
                    return messages[:resolved_limit]

            page_token = listed.get("nextPageToken")
            if not page_token:
                return messages

    def _download_message(self, message_id: str, operation: str) -> DownloadedMessage:
        started = time.perf_counter()
        message = execute_google_with_retry(
            lambda: self.gmail.users().messages().get(userId="me", id=message_id, format="full").execute(),
            operation=operation,
        )
        subject = _message_subject(message)
        zip_attachments = self._extract_zip_attachments(message_id, message)
        elapsed_ms = (time.perf_counter() - started) * 1000
        return DownloadedMessage(
            message_id=message_id,
            subject=subject,
            attachments=zip_attachments,
            download_ms=elapsed_ms,
            bytes_processed=sum(len(raw_data) for _, raw_data in zip_attachments),
        )

    def _process_downloaded_message(
        self,
        message: DownloadedMessage,
        runtime: RuntimeOptions,
    ) -> MessageProcessingOutcome:
        summary = PollSummary(
            gmail_download_ms=message.download_ms,
            bytes_processed=message.bytes_processed,
        )
        if not message.attachments:
            LOGGER.info("Mensaje sin ZIP valido. id=%s subject=%s", message.message_id, message.subject)
            summary.skipped_messages = 1
            return MessageProcessingOutcome(
                message_id=message.message_id,
                should_mark_processed=True,
                summary=summary,
            )

        ok_count = 0
        failed = False
        for attachment_name, raw_data in message.attachments:
            try:
                result, sync_metrics = self._process_zip_attachment(
                    message_id=message.message_id,
                    attachment_name=attachment_name,
                    data=raw_data,
                    runtime=runtime,
                )
                ok_count += 1
                summary.processed_attachments += 1
                summary.parse_ms += result.metrics.parse_ms
                summary.pricing_ms += result.metrics.pricing_ms
                summary.artifact_ms += result.metrics.artifact_ms
                summary.drive_ms += sync_metrics.drive_ms
                summary.erp_ms += sync_metrics.erp_ms
                summary.gcs_ms += sync_metrics.gcs_ms
            except Exception as exc:
                if _is_skippable_attachment_error(exc):
                    LOGGER.warning(
                        "Adjunto ZIP omitido por tipo no soportado. message_id=%s attachment=%s detalle=%s",
                        message.message_id,
                        attachment_name,
                        exc,
                    )
                    continue
                failed = True
                LOGGER.exception(
                    "No se pudo procesar adjunto ZIP. message_id=%s attachment=%s error=%s",
                    message.message_id,
                    attachment_name,
                    exc,
                )

        if failed:
            summary.failed_messages = 1
            return MessageProcessingOutcome(
                message_id=message.message_id,
                should_mark_processed=False,
                summary=summary,
            )

        summary.processed_messages = 1
        return MessageProcessingOutcome(
            message_id=message.message_id,
            should_mark_processed=True,
            summary=summary,
        )

    def _extract_zip_attachments(self, message_id: str, message_payload: dict) -> list[tuple[str, bytes]]:
        attachments: list[tuple[str, bytes]] = []
        payload = message_payload.get("payload") or {}
        for part in _iter_parts(payload):
            filename = str(part.get("filename", "")).strip()
            if not filename.lower().endswith(".zip"):
                continue

            body = part.get("body") or {}
            encoded = body.get("data")
            attachment_id = body.get("attachmentId")
            if attachment_id:
                response = execute_google_with_retry(
                    lambda: self.gmail.users().messages().attachments().get(
                        userId="me",
                        messageId=message_id,
                        id=attachment_id,
                    ).execute(),
                    operation="gmail.messages.attachments.get",
                )
                encoded = response.get("data")

            if not encoded:
                continue

            raw = base64.urlsafe_b64decode(encoded.encode("utf-8"))
            attachments.append((_safe_name(filename), raw))

        return attachments

    def _extract_invoice_ref_from_zip_bytes(self, zip_data: bytes, fallback_name: str) -> str:
        with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zf:
            xml_entries = [entry for entry in zf.infolist() if not entry.is_dir() and entry.filename.lower().endswith(".xml")]
            if not xml_entries:
                raise ValueError("ZIP sin XML para extraer referencia de factura.")

            last_error = None
            for entry in xml_entries:
                try:
                    invoice_root = extract_invoice_root_from_bytes(zf.read(entry))
                    header = parse_invoice_header(invoice_root)
                    if header.invoice_id:
                        return _safe_name(header.invoice_id)
                    return _safe_name(Path(entry.filename).stem)
                except Exception as exc:
                    last_error = exc

        LOGGER.warning("No se pudo extraer invoice_id del ZIP (%s). Se usa fallback.", last_error)
        return _safe_name(Path(fallback_name).stem)

    def _sync_ingresado_message(self, message_id: str) -> int:
        downloaded = self._download_message(message_id=message_id, operation="gmail.messages.get.ingresado")
        zip_attachments = downloaded.attachments
        if not zip_attachments:
            LOGGER.info("Mensaje 'Ingresado' sin adjuntos ZIP. id=%s", message_id)
            return 0

        ingresado_parent_id = self._ensure_drive_folder(
            self.config.entered_drive_subfolder_name,
            self.config.drive_parent_folder_id,
        )
        moved = 0
        for attachment_name, data in zip_attachments:
            invoice_ref = self._extract_invoice_ref_from_zip_bytes(data, fallback_name=attachment_name)
            if self._move_drive_folder(invoice_ref, ingresado_parent_id):
                moved += 1
        return moved

    def _process_zip_attachment(
        self,
        message_id: str,
        attachment_name: str,
        data: bytes,
        runtime: RuntimeOptions,
    ) -> tuple[ProcessResult, AttachmentSyncMetrics]:
        output_base = self.config.local_work_dir / "output"
        result = process_invoice_bytes(
            input_name=attachment_name,
            input_bytes=data,
            output_path=output_base if not runtime.skip_drive else None,
            config=self.config.pricing_config(),
            sheet_name=self.config.sheet_name,
            rules_path=self.config.rules_path,
            generate_output=not runtime.skip_drive,
        )

        sync_metrics = AttachmentSyncMetrics()
        if not runtime.skip_drive and result.output_path is not None:
            drive_started = time.perf_counter()
            self._sync_folder_to_drive(result.output_path)
            sync_metrics.drive_ms = (time.perf_counter() - drive_started) * 1000

        erp_ms, gcs_ms = self._post_to_erp(result=result, message_id=message_id)
        sync_metrics.erp_ms = erp_ms
        sync_metrics.gcs_ms = gcs_ms
        return result, sync_metrics

    def _build_erp_payload(self, result: ProcessResult) -> dict:
        if not result.header:
            raise AutomationError("Resultado de factura sin header para ERP.")

        header = result.header
        lines = []
        if result.price_rows:
            for i, price_row in enumerate(result.price_rows, 1):
                discount_factor = Decimal("1") - (price_row.discount_percent / Decimal("100"))
                unit_cost_after_discount = price_row.cost_bruto_unit * discount_factor
                line_total = price_row.cost_neto_unit * price_row.quantity
                try:
                    line_number = int(str(price_row.source_line_id).strip())
                except Exception:
                    line_number = i
                lines.append({
                    "lineNumber": line_number,
                    "description": price_row.product,
                    "supplierReference": price_row.supplier_reference or None,
                    "quantity": float(price_row.quantity),
                    "unitCostBeforeDiscount": int(round(price_row.cost_bruto_unit)),
                    "discountPercent": float(price_row.discount_percent),
                    "unitCostAfterDiscount": int(round(unit_cost_after_discount)),
                    "taxPercent": float(price_row.tax_percent),
                    "unitCostIncTax": int(round(price_row.cost_neto_unit)),
                    "lineTotal": int(round(line_total)),
                    "suggestedPriceIncTax": int(round(price_row.venta_neta_unit)),
                })

        return {
            "documentKind": header.document_kind or "PURCHASE_INVOICE",
            "supplier": {
                "nit": header.supplier_id or "",
                "name": header.supplier_name or "",
            },
            "invoice": {
                "invoiceNumber": header.invoice_id or "",
                "cufe": header.cufe or "",
                "referenceInvoiceNumber": header.reference_invoice_number or "",
                "referenceCufe": header.reference_cufe or "",
                "issueDate": header.issue_date or "",
                "dueDate": header.due_date or "",
                "subtotal": int(round(header.subtotal)),
                "taxTotal": int(round(header.tax_total)),
                "total": int(round(header.total)),
            },
            "lines": lines,
        }

    def _get_storage_client(self):
        if self._storage_client is None:
            storage = _import_google_storage_dep()
            # GCS debe autenticarse con ADC/service account del runtime, no con el token OAuth de Gmail.
            self._storage_client = storage.Client()
        return self._storage_client

    def _upload_invoice_artifacts_to_gcs(self, message_id: str, result: ProcessResult) -> dict:
        if not self.config.artifacts_bucket_name:
            return {}

        storage_client = self._get_storage_client()
        bucket = storage_client.bucket(self.config.artifacts_bucket_name)
        invoice_ref = _safe_name(result.invoice_ref or "Factura")
        message_ref = _safe_name(message_id)
        prefix_parts = [self.config.artifacts_prefix.strip("/"), invoice_ref, message_ref]
        base_prefix = "/".join(part for part in prefix_parts if part)

        artifacts: dict[str, str] = {}
        if result.raw_xml:
            xml_name = f"{base_prefix}/{invoice_ref}.xml"
            bucket.blob(xml_name).upload_from_string(
                result.raw_xml.encode("utf-8"),
                content_type="application/xml; charset=utf-8",
            )
            artifacts["xmlGcsUri"] = f"gs://{bucket.name}/{xml_name}"

        if result.pdf_bytes:
            pdf_name = _safe_name(result.pdf_filename or f"{invoice_ref}.pdf")
            pdf_object = f"{base_prefix}/{pdf_name}"
            bucket.blob(pdf_object).upload_from_string(result.pdf_bytes, content_type="application/pdf")
            artifacts["pdfGcsUri"] = f"gs://{bucket.name}/{pdf_object}"
            artifacts["pdfFilename"] = result.pdf_filename or pdf_name

        return artifacts

    def _post_to_erp(self, result: ProcessResult, message_id: str) -> tuple[float, float]:
        if not self.config.erp_base_url or not result.header:
            return 0.0, 0.0

        payload = self._build_erp_payload(result)
        gcs_ms = 0.0
        artifacts = {}
        if self.config.artifacts_bucket_name:
            gcs_started = time.perf_counter()
            artifacts = self._upload_invoice_artifacts_to_gcs(message_id=message_id, result=result)
            gcs_ms = (time.perf_counter() - gcs_started) * 1000
            if artifacts:
                payload["artifacts"] = artifacts

        if not artifacts:
            if result.pdf_bytes:
                payload["pdfBase64"] = base64.b64encode(result.pdf_bytes).decode("ascii")
                payload["pdfFilename"] = result.pdf_filename or "invoice.pdf"
            if result.raw_xml:
                payload["xmlRaw"] = result.raw_xml

        url = f"{self.config.erp_base_url.rstrip('/')}/api/purchases/ingest"
        req_data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=req_data, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.config.erp_api_key:
            req.add_header("X-API-Key", self.config.erp_api_key)

        started = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                status = response.status
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                pass
            raise AutomationError(
                f"ERP devolvio HTTP {exc.code} para invoice={result.header.invoice_id or '?'} body={body}"
            ) from exc
        except Exception as exc:
            raise AutomationError(
                f"ERP ingestion failed para invoice={result.header.invoice_id or '?'}: {exc}"
            ) from exc

        erp_ms = (time.perf_counter() - started) * 1000
        LOGGER.info(
            "ERP ingestion OK: invoice=%s status=%s response=%s",
            result.header.invoice_id,
            status,
            body[:500],
        )
        return erp_ms, gcs_ms

    def _sync_folder_to_drive(self, local_folder: Path) -> None:
        if not local_folder.exists() or not local_folder.is_dir():
            raise AutomationError(f"Carpeta local invalida para subida: {local_folder}")

        with self._drive_lock:
            folder_id = self._ensure_drive_folder(local_folder.name, self.config.drive_parent_folder_id)
            for item in local_folder.iterdir():
                if not item.is_file():
                    continue
                self._upload_file_if_missing(item, folder_id)

    def _cache_drive_folder(self, folder_name: str, parent_id: str, folder_id: str) -> str:
        self._drive_folder_cache[(parent_id, folder_name)] = folder_id
        return folder_id

    def _list_drive_folder_file_names(self, drive_folder_id: str) -> set[str]:
        cached = self._drive_folder_files_cache.get(drive_folder_id)
        if cached is not None:
            return cached

        names: set[str] = set()
        page_token: Optional[str] = None
        escaped_parent = _escape_drive_query_value(drive_folder_id)
        query = f"'{escaped_parent}' in parents and trashed=false"

        while True:
            request_kwargs = {
                "q": query,
                "spaces": "drive",
                "fields": "nextPageToken,files(name)",
                "pageSize": 200,
            }
            if page_token:
                request_kwargs["pageToken"] = page_token

            listed = execute_google_with_retry(
                lambda request_kwargs=request_kwargs: self.drive.files().list(**request_kwargs).execute(),
                operation="drive.files.list.folder_contents",
            )
            for file_info in listed.get("files", []):
                name = str(file_info.get("name") or "").strip()
                if name:
                    names.add(name)

            page_token = listed.get("nextPageToken")
            if not page_token:
                self._drive_folder_files_cache[drive_folder_id] = names
                return names

    def _find_drive_folder(self, folder_name: str, parent_id: str) -> Optional[str]:
        cached = self._drive_folder_cache.get((parent_id, folder_name))
        if cached:
            return cached

        escaped_name = _escape_drive_query_value(folder_name)
        escaped_parent = _escape_drive_query_value(parent_id)
        query = (
            "mimeType='application/vnd.google-apps.folder' "
            f"and name='{escaped_name}' and '{escaped_parent}' in parents and trashed=false"
        )
        listed = execute_google_with_retry(
            lambda: self.drive.files().list(q=query, spaces="drive", fields="files(id,name)", pageSize=1).execute(),
            operation="drive.files.list.find_folder",
        )
        files = listed.get("files", [])
        if files:
            return self._cache_drive_folder(folder_name, parent_id, str(files[0]["id"]))
        return None

    def _ensure_drive_folder(self, folder_name: str, parent_id: str) -> str:
        existing_id = self._find_drive_folder(folder_name, parent_id)
        if existing_id:
            return existing_id

        created = execute_google_with_retry(
            lambda: self.drive.files().create(
                body={
                    "name": folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                },
                fields="id,name",
            ).execute(),
            operation="drive.files.create.folder",
        )
        folder_id = str(created["id"])
        self._cache_drive_folder(folder_name, parent_id, folder_id)
        self._drive_folder_files_cache[folder_id] = set()
        return folder_id

    def _move_drive_folder(self, folder_name: str, target_parent_id: str) -> bool:
        with self._drive_lock:
            source_folder_id = self._find_drive_folder(folder_name, self.config.drive_parent_folder_id)
            if source_folder_id is None:
                already_target_id = self._find_drive_folder(folder_name, target_parent_id)
                if already_target_id is not None:
                    LOGGER.info("Carpeta ya ubicada en '%s': %s", self.config.entered_drive_subfolder_name, folder_name)
                else:
                    LOGGER.warning("No se encontro carpeta en Drive para mover: %s", folder_name)
                return False

            metadata = execute_google_with_retry(
                lambda: self.drive.files().get(fileId=source_folder_id, fields="id,name,parents").execute(),
                operation="drive.files.get.folder_move",
            )
            parents = [str(item) for item in metadata.get("parents", []) if item]
            if target_parent_id in parents and len(parents) == 1:
                return False

            remove_parents = ",".join(parent for parent in parents if parent != target_parent_id)
            execute_google_with_retry(
                lambda: self.drive.files().update(
                    fileId=source_folder_id,
                    addParents=target_parent_id,
                    removeParents=remove_parents,
                    fields="id,parents",
                ).execute(),
                operation="drive.files.update.move_folder",
            )
            self._drive_folder_cache.pop((self.config.drive_parent_folder_id, folder_name), None)
            self._cache_drive_folder(folder_name, target_parent_id, source_folder_id)
            LOGGER.info("Carpeta movida a '%s': %s", self.config.entered_drive_subfolder_name, folder_name)
            return True

    def _upload_file_if_missing(self, local_file: Path, drive_folder_id: str) -> None:
        _, _, _, _, MediaFileUpload = _import_google_deps()
        existing_file_names = self._list_drive_folder_file_names(drive_folder_id)
        if local_file.name in existing_file_names:
            LOGGER.info("Archivo ya existe en Drive, se conserva: %s", local_file.name)
            return

        media = MediaFileUpload(str(local_file), resumable=False)
        execute_google_with_retry(
            lambda: self.drive.files().create(
                body={
                    "name": local_file.name,
                    "parents": [drive_folder_id],
                },
                media_body=media,
                fields="id,name",
            ).execute(),
            operation="drive.files.create.file",
        )
        existing_file_names.add(local_file.name)
        LOGGER.info("Archivo subido a Drive: %s", local_file)

    def _mark_message_processed(self, message_id: str) -> float:
        remove = ["UNREAD"] if self.config.mark_as_read else []
        started = time.perf_counter()
        execute_google_with_retry(
            lambda: self.gmail.users().messages().modify(
                userId="me",
                id=message_id,
                body={
                    "addLabelIds": [self.processed_label_id],
                    "removeLabelIds": remove,
                },
            ).execute(),
            operation="gmail.messages.modify",
        )
        return (time.perf_counter() - started) * 1000

    def _mark_ingresado_synced(self, message_id: str) -> None:
        if not self.entered_synced_label_id:
            return
        execute_google_with_retry(
            lambda: self.gmail.users().messages().modify(
                userId="me",
                id=message_id,
                body={
                    "addLabelIds": [self.entered_synced_label_id],
                    "removeLabelIds": [],
                },
            ).execute(),
            operation="gmail.messages.modify.ingresado_synced",
        )
