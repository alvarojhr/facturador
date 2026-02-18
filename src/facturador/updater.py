from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any, Optional
from urllib import request


class UpdateError(Exception):
    pass


@dataclass
class UpdateConfig:
    manifest_url: str = ""
    check_on_startup: bool = True
    auto_install: bool = False
    request_timeout_sec: int = 8
    silent_install_args: list[str] = field(
        default_factory=lambda: ["/VERYSILENT", "/NORESTART", "/CLOSEAPPLICATIONS"]
    )


@dataclass
class UpdateInfo:
    version: str
    installer_url: str
    sha256: str
    notes: str = ""
    mandatory: bool = False


def _default_update_config_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parents[2]
    return base / "config" / "update_config.json"


def load_update_config(path: Optional[Path] = None) -> UpdateConfig:
    cfg_path = path or _default_update_config_path()
    if not cfg_path.exists():
        return UpdateConfig()

    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise UpdateError(f"No se pudo leer {cfg_path}: {exc}") from exc

    cfg = UpdateConfig()
    if isinstance(data.get("manifest_url"), str):
        cfg.manifest_url = data["manifest_url"].strip()
    if isinstance(data.get("check_on_startup"), bool):
        cfg.check_on_startup = data["check_on_startup"]
    if isinstance(data.get("auto_install"), bool):
        cfg.auto_install = data["auto_install"]
    if isinstance(data.get("request_timeout_sec"), int):
        cfg.request_timeout_sec = data["request_timeout_sec"]
    if isinstance(data.get("silent_install_args"), list):
        cfg.silent_install_args = [str(item) for item in data["silent_install_args"]]
    return cfg


def _version_tuple(value: str) -> tuple[int, ...]:
    # Extrae solo componentes numericos para comparar semanticamente.
    parts = re.findall(r"\d+", value or "")
    if not parts:
        return (0,)
    nums = [int(p) for p in parts[:4]]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)


def _is_newer(remote: str, current: str) -> bool:
    return _version_tuple(remote) > _version_tuple(current)


def _read_manifest(url: str, timeout: int) -> dict[str, Any]:
    req = request.Request(url, headers={"User-Agent": "Facturador-Updater/1.0"})
    with request.urlopen(req, timeout=timeout) as resp:
        content_type = resp.headers.get("Content-Type", "")
        payload = resp.read()
    if payload is None:
        raise UpdateError("Manifest vacio.")
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise UpdateError(f"Manifest no es JSON valido ({content_type}).") from exc
    if not isinstance(data, dict):
        raise UpdateError("Manifest invalido: se esperaba un objeto JSON.")
    return data


def check_for_update(current_version: str, cfg: UpdateConfig) -> Optional[UpdateInfo]:
    if not cfg.manifest_url:
        return None

    data = _read_manifest(cfg.manifest_url, cfg.request_timeout_sec)
    remote_version = str(data.get("version") or data.get("latest_version") or "").strip()
    installer_url = str(data.get("installer_url") or "").strip()
    sha256 = str(data.get("sha256") or "").strip().lower()
    notes = str(data.get("notes") or "")
    mandatory = bool(data.get("mandatory") or False)

    if not remote_version or not installer_url:
        raise UpdateError("Manifest invalido: faltan 'version' o 'installer_url'.")
    if not _is_newer(remote_version, current_version):
        return None
    return UpdateInfo(
        version=remote_version,
        installer_url=installer_url,
        sha256=sha256,
        notes=notes,
        mandatory=mandatory,
    )


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest().lower()


def download_update_installer(info: UpdateInfo) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="facturador_update_"))
    file_name = Path(info.installer_url.split("?", 1)[0]).name or f"FacturadorSetup_{info.version}.exe"
    target = temp_dir / file_name
    req = request.Request(info.installer_url, headers={"User-Agent": "Facturador-Updater/1.0"})
    with request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    with open(target, "wb") as handle:
        handle.write(data)

    if info.sha256:
        digest = _hash_file(target)
        if digest != info.sha256:
            raise UpdateError("Hash SHA256 no coincide con el manifest.")
    return target


def launch_installer(installer_path: Path, args: list[str]) -> None:
    cmd = [str(installer_path), *args]
    try:
        subprocess.Popen(cmd, close_fds=True)
    except Exception as exc:
        raise UpdateError(f"No se pudo ejecutar el instalador: {exc}") from exc
