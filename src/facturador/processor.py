from pathlib import Path
from typing import Optional
import zipfile
import sys
from dataclasses import dataclass

from .invoice_parser import (
    extract_invoice_root,
    extract_invoice_root_from_bytes,
    parse_invoice_header,
    parse_invoice_lines,
)
from .pricing import MarkupConfig, build_price_rows
from .rules import load_rules
from .excel_writer import export_price_rows


@dataclass
class ProcessResult:
    output_path: Path
    skipped_existing: bool = False


def _safe_name(value: str) -> str:
    if not value:
        return "Factura"
    invalid = '<>:"/\\\\|?*'
    cleaned = "".join("_" if ch in invalid or ord(ch) < 32 else ch for ch in value)
    cleaned = cleaned.strip().strip(".")
    return cleaned or "Factura"


def _load_from_zip(input_path: Path):
    with zipfile.ZipFile(input_path, "r") as zf:
        xml_entries = [info for info in zf.infolist() if not info.is_dir() and info.filename.lower().endswith(".xml")]
        if not xml_entries:
            raise FileNotFoundError("No se encontro ningun XML dentro del ZIP.")

        pdf_entries = [info for info in zf.infolist() if not info.is_dir() and info.filename.lower().endswith(".pdf")]
        pdf_info = pdf_entries[0] if pdf_entries else None

        last_error = None
        for info in xml_entries:
            try:
                data = zf.read(info)
                invoice_root = extract_invoice_root_from_bytes(data)
                pdf_bytes = zf.read(pdf_info) if pdf_info else None
                return invoice_root, info.filename, pdf_info.filename if pdf_info else None, pdf_bytes
            except Exception as exc:
                last_error = exc

    raise ValueError(f"No se pudo leer un XML de factura valido dentro del ZIP. Ultimo error: {last_error}")


def load_invoice_root(input_path: Path):
    if input_path.suffix.lower() != ".zip":
        return extract_invoice_root(str(input_path)), input_path.name, None, None
    return _load_from_zip(input_path)


def _default_rules_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parents[2]
    return base / "config" / "reglas_especiales.xlsx"


def process_invoice(
    input_path: Path,
    output_path: Optional[Path],
    config: MarkupConfig,
    sheet_name: str = "Productos",
    rules_path: Optional[Path] = None,
) -> ProcessResult:
    invoice_root, xml_name, pdf_name, pdf_bytes = load_invoice_root(input_path)
    invoice_lines = parse_invoice_lines(invoice_root)
    invoice_header = parse_invoice_header(invoice_root)
    rules_file = rules_path or _default_rules_path()
    rules = load_rules(rules_file) if rules_file else []
    price_rows = build_price_rows(invoice_lines, config, rules=rules)

    invoice_ref = _safe_name(invoice_header.invoice_id) if invoice_header.invoice_id else _safe_name(Path(xml_name).stem)

    if input_path.suffix.lower() == ".zip":
        # output_path se interpreta como carpeta base
        if output_path is None:
            base_dir = input_path.parent
        else:
            base_dir = output_path if output_path.suffix.lower() != ".xlsx" else output_path.parent
        target_dir = base_dir / invoice_ref
        target_dir.mkdir(parents=True, exist_ok=True)

        excel_path = target_dir / f"{invoice_ref}.xlsx"
        skipped_existing = False
        if excel_path.exists():
            skipped_existing = True
        else:
            export_price_rows(price_rows, excel_path, sheet_name=sheet_name, header=invoice_header, config=config)

        if pdf_bytes and pdf_name:
            pdf_path = target_dir / _safe_name(Path(pdf_name).name)
            if not pdf_path.exists():
                with open(pdf_path, "wb") as handle:
                    handle.write(pdf_bytes)

        return ProcessResult(output_path=target_dir, skipped_existing=skipped_existing)

    # XML directo: si output_path es carpeta, crear subcarpeta con referencia y poner XLSX adentro
    if output_path is None:
        output_path = input_path.with_suffix(".xlsx")
        if output_path.exists():
            return ProcessResult(output_path=output_path, skipped_existing=True)
        export_price_rows(price_rows, output_path, sheet_name=sheet_name, header=invoice_header, config=config)
        return ProcessResult(output_path=output_path, skipped_existing=False)

    if output_path.suffix.lower() == ".xlsx":
        if output_path.exists():
            return ProcessResult(output_path=output_path, skipped_existing=True)
        export_price_rows(price_rows, output_path, sheet_name=sheet_name, header=invoice_header, config=config)
        return ProcessResult(output_path=output_path, skipped_existing=False)

    target_dir = output_path / invoice_ref
    target_dir.mkdir(parents=True, exist_ok=True)
    excel_path = target_dir / f"{invoice_ref}.xlsx"
    if excel_path.exists():
        return ProcessResult(output_path=target_dir, skipped_existing=True)
    export_price_rows(price_rows, excel_path, sheet_name=sheet_name, header=invoice_header, config=config)
    return ProcessResult(output_path=target_dir, skipped_existing=False)
