import base64
from dataclasses import dataclass
from decimal import Decimal
import json
import logging
from pathlib import Path
import socket
import ssl
import sys
import time
from typing import Optional

from googleapiclient.errors import HttpError

from .pricing import MarkupConfig
from .processor import ProcessResult, process_invoice


LOGGER = logging.getLogger(__name__)

GMAIL_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive",
]


class AutomationError(Exception):
    pass


@dataclass
class MailAutomationConfig:
    gmail_query: str = "has:attachment filename:zip in:inbox"
    processed_label_name: str = "facturador-procesado"
    mark_as_read: bool = True
    poll_interval_sec: int = 60
    max_messages_per_poll: int = 20
    drive_parent_folder_id: str = ""
    credentials_path: Path = Path("config/google_credentials.json")
    token_path: Path = Path("config/google_token.json")
    local_work_dir: Path = Path("automation_work")
    rules_path: Optional[Path] = None
    sheet_name: str = "Productos"
    markup_threshold: Decimal = Decimal("10000")
    markup_below: Decimal = Decimal("0.68")
    markup_above: Decimal = Decimal("1.32")
    round_net_step: Decimal = Decimal("100")
    rounding_mode: str = "up"

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
    cfg.max_messages_per_poll = int(payload.get("max_messages_per_poll", cfg.max_messages_per_poll))
    cfg.drive_parent_folder_id = str(payload.get("drive_parent_folder_id", "")).strip()

    credentials = _resolve_optional_path(base, payload.get("credentials_path"))
    token = _resolve_optional_path(base, payload.get("token_path"))
    local_dir = _resolve_optional_path(base, payload.get("local_work_dir"))
    rules = _resolve_optional_path(base, payload.get("rules_path"))

    cfg.credentials_path = credentials or (base / "config" / "google_credentials.json")
    cfg.token_path = token or (base / "config" / "google_token.json")
    cfg.local_work_dir = local_dir or (base / "automation_work")
    cfg.rules_path = rules

    cfg.sheet_name = str(payload.get("sheet_name", cfg.sheet_name)).strip() or cfg.sheet_name
    cfg.markup_threshold = Decimal(str(payload.get("markup_threshold", cfg.markup_threshold)))
    cfg.markup_below = Decimal(str(payload.get("markup_below", cfg.markup_below)))
    cfg.markup_above = Decimal(str(payload.get("markup_above", cfg.markup_above)))
    cfg.round_net_step = Decimal(str(payload.get("round_net_step", cfg.round_net_step)))
    cfg.rounding_mode = str(payload.get("rounding_mode", cfg.rounding_mode)).strip() or cfg.rounding_mode

    if cfg.poll_interval_sec < 10:
        raise AutomationError("poll_interval_sec debe ser >= 10 segundos.")
    if cfg.max_messages_per_poll < 1:
        raise AutomationError("max_messages_per_poll debe ser >= 1.")
    if not cfg.drive_parent_folder_id:
        raise AutomationError("Configura drive_parent_folder_id con el ID de la carpeta destino en Google Drive.")
    if cfg.rounding_mode not in {"up", "down", "nearest"}:
        raise AutomationError("rounding_mode invalido. Debe ser: up, down o nearest.")

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
        "no se encontro un xml de invoice embebido",
        "la invoice no contiene lineas de items",
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
        self.gmail, self.drive = self._build_google_services()
        self.processed_label_id = self._ensure_gmail_label(self.config.processed_label_name)

    def _build_google_services(self):
        Request, Credentials, InstalledAppFlow, build, _ = _import_google_deps()

        creds = None
        if self.config.token_path.exists():
            creds = Credentials.from_authorized_user_file(str(self.config.token_path), GMAIL_DRIVE_SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not self.config.credentials_path.exists():
                    raise FileNotFoundError(
                        "No se encontro credentials de Google OAuth.\n"
                        f"Ruta esperada: {self.config.credentials_path}"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(str(self.config.credentials_path), GMAIL_DRIVE_SCOPES)
                creds = flow.run_local_server(port=0)

            try:
                self.config.token_path.parent.mkdir(parents=True, exist_ok=True)
                self.config.token_path.write_text(creds.to_json(), encoding="utf-8")
            except OSError as exc:
                LOGGER.warning(
                    "No se pudo persistir token OAuth en %s (%s). Se continua con credenciales en memoria.",
                    self.config.token_path,
                    exc,
                )

        gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        return gmail, drive

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

    def run_forever(self) -> None:
        LOGGER.info("Automatizacion Gmail/Drive iniciada. Intervalo: %ss", self.config.poll_interval_sec)
        while True:
            try:
                summary = self.run_once()
                LOGGER.info(
                    "Ciclo completado. mensajes=%s procesados=%s adjuntos=%s fallidos=%s",
                    summary.checked_messages,
                    summary.processed_messages,
                    summary.processed_attachments,
                    summary.failed_messages,
                )
            except Exception as exc:
                LOGGER.exception("Fallo en ciclo de automatizacion: %s", exc)
            time.sleep(self.config.poll_interval_sec)

    def run_once(self) -> PollSummary:
        summary = PollSummary()
        messages = self.query_unprocessed_messages(limit=self.config.max_messages_per_poll)
        summary.checked_messages = len(messages)

        if not messages:
            return summary

        for msg_ref in messages:
            msg_id = str(msg_ref.get("id"))
            if not msg_id:
                continue

            ok_count, skip_count, failed = self._process_message(msg_id)
            summary.processed_attachments += ok_count
            summary.skipped_messages += skip_count
            if failed:
                summary.failed_messages += 1
            else:
                summary.processed_messages += 1

        return summary

    def query_unprocessed_messages(self, limit: Optional[int] = None) -> list[dict]:
        query = f"({self.config.gmail_query}) -label:{self.config.processed_label_name}"
        listed = execute_google_with_retry(
            lambda: self.gmail.users().messages().list(
                userId="me",
                q=query,
                maxResults=limit or self.config.max_messages_per_poll,
            ).execute(),
            operation="gmail.messages.list",
        )
        return listed.get("messages", [])

    def process_message_by_id(self, message_id: str) -> tuple[int, int, bool]:
        return self._process_message(message_id)

    def drain_unprocessed_messages(self, max_cycles: int = 20) -> PollSummary:
        merged = PollSummary()
        for _ in range(max_cycles):
            cycle = self.run_once()
            merged.checked_messages += cycle.checked_messages
            merged.processed_messages += cycle.processed_messages
            merged.processed_attachments += cycle.processed_attachments
            merged.failed_messages += cycle.failed_messages
            merged.skipped_messages += cycle.skipped_messages
            if cycle.checked_messages == 0:
                break
        return merged

    def _process_message(self, message_id: str) -> tuple[int, int, bool]:
        message = execute_google_with_retry(
            lambda: self.gmail.users().messages().get(userId="me", id=message_id, format="full").execute(),
            operation="gmail.messages.get",
        )
        subject = _message_subject(message)
        zip_attachments = self._extract_zip_attachments(message_id, message)
        if not zip_attachments:
            LOGGER.info("Mensaje sin ZIP valido. id=%s subject=%s", message_id, subject)
            self._mark_message_processed(message_id)
            return 0, 1, False

        ok_count = 0
        failed = False
        for att_name, raw_data in zip_attachments:
            try:
                self._process_zip_attachment(message_id, att_name, raw_data)
                ok_count += 1
            except Exception as exc:
                if _is_skippable_attachment_error(exc):
                    LOGGER.warning(
                        "Adjunto ZIP omitido por tipo no soportado. message_id=%s attachment=%s detalle=%s",
                        message_id,
                        att_name,
                        exc,
                    )
                    continue
                failed = True
                LOGGER.exception(
                    "No se pudo procesar adjunto ZIP. message_id=%s attachment=%s error=%s",
                    message_id,
                    att_name,
                    exc,
                )

        if not failed:
            self._mark_message_processed(message_id)
        return ok_count, 0, failed

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

    def _process_zip_attachment(self, message_id: str, attachment_name: str, data: bytes) -> ProcessResult:
        incoming_dir = self.config.local_work_dir / "incoming"
        zip_path = incoming_dir / f"{message_id}_{_safe_name(attachment_name)}"
        with open(zip_path, "wb") as handle:
            handle.write(data)

        output_base = self.config.local_work_dir / "output"
        result = process_invoice(
            zip_path,
            output_base,
            self.config.pricing_config(),
            sheet_name=self.config.sheet_name,
            rules_path=self.config.rules_path,
        )
        self._sync_folder_to_drive(result.output_path)
        try:
            zip_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.warning("No se pudo eliminar temporal ZIP: %s", zip_path)
        return result

    def _sync_folder_to_drive(self, local_folder: Path) -> None:
        if not local_folder.exists() or not local_folder.is_dir():
            raise AutomationError(f"Carpeta local invalida para subida: {local_folder}")

        folder_id = self._ensure_drive_folder(local_folder.name, self.config.drive_parent_folder_id)
        for item in local_folder.iterdir():
            if not item.is_file():
                continue
            self._upload_file_if_missing(item, folder_id)

    def _ensure_drive_folder(self, folder_name: str, parent_id: str) -> str:
        escaped_name = _escape_drive_query_value(folder_name)
        escaped_parent = _escape_drive_query_value(parent_id)
        query = (
            "mimeType='application/vnd.google-apps.folder' "
            f"and name='{escaped_name}' and '{escaped_parent}' in parents and trashed=false"
        )
        listed = execute_google_with_retry(
            lambda: self.drive.files().list(q=query, spaces="drive", fields="files(id,name)", pageSize=1).execute(),
            operation="drive.files.list.folder",
        )
        files = listed.get("files", [])
        if files:
            return str(files[0]["id"])

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
        return str(created["id"])

    def _upload_file_if_missing(self, local_file: Path, drive_folder_id: str) -> None:
        _, _, _, _, MediaFileUpload = _import_google_deps()
        escaped_name = _escape_drive_query_value(local_file.name)
        escaped_parent = _escape_drive_query_value(drive_folder_id)
        query = f"name='{escaped_name}' and '{escaped_parent}' in parents and trashed=false"
        listed = execute_google_with_retry(
            lambda: self.drive.files().list(q=query, spaces="drive", fields="files(id,name)", pageSize=1).execute(),
            operation="drive.files.list.file",
        )
        existing = listed.get("files", [])
        if existing:
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
        LOGGER.info("Archivo subido a Drive: %s", local_file)

    def _mark_message_processed(self, message_id: str) -> None:
        remove = ["UNREAD"] if self.config.mark_as_read else []
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
