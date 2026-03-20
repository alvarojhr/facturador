from dataclasses import dataclass, field
import io
from pathlib import Path
import sys
import time
from typing import List, Optional
import zipfile

from .invoice_parser import (
    InvoiceHeader,
    extract_invoice_root_from_bytes,
    parse_invoice_header,
    parse_invoice_lines,
)
from .pricing import MarkupConfig, PriceRow, build_price_rows
from .rules import load_rules
from .excel_writer import export_price_rows


@dataclass
class ProcessResult:
    output_path: Optional[Path]
    skipped_existing: bool = False
    header: Optional[InvoiceHeader] = None
    price_rows: Optional[List[PriceRow]] = None
    pdf_bytes: Optional[bytes] = None
    pdf_filename: Optional[str] = None
    raw_xml: Optional[str] = None
    invoice_ref: str = ""
    metrics: "ProcessMetrics" = field(default_factory=lambda: ProcessMetrics())


@dataclass
class ProcessMetrics:
    parse_ms: float = 0.0
    pricing_ms: float = 0.0
    artifact_ms: float = 0.0


def _safe_name(value: str) -> str:
    if not value:
        return "Factura"
    invalid = '<>:"/\\\\|?*'
    cleaned = "".join("_" if ch in invalid or ord(ch) < 32 else ch for ch in value)
    cleaned = cleaned.strip().strip(".")
    return cleaned or "Factura"


def _load_from_zip_stream(stream: io.BytesIO):
    with zipfile.ZipFile(stream, "r") as zf:
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
                return invoice_root, info.filename, pdf_info.filename if pdf_info else None, pdf_bytes, data
            except Exception as exc:
                last_error = exc

    raise ValueError(f"No se pudo leer un XML de factura valido dentro del ZIP. Ultimo error: {last_error}")


def load_invoice_root(input_path: Path):
    return load_invoice_root_bytes(input_path.name, input_path.read_bytes())


def load_invoice_root_bytes(input_name: str, input_bytes: bytes):
    if not input_name.lower().endswith(".zip"):
        return extract_invoice_root_from_bytes(input_bytes), input_name, None, None, input_bytes
    return _load_from_zip_stream(io.BytesIO(input_bytes))


def _default_rules_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parents[2]
    return base / "config" / "reglas_especiales.xlsx"


def _write_invoice_artifacts(
    input_name: str,
    output_path: Optional[Path],
    sheet_name: str,
    header: InvoiceHeader,
    price_rows: List[PriceRow],
    config: MarkupConfig,
    pdf_bytes: Optional[bytes],
    pdf_name: Optional[str],
    invoice_ref: str,
) -> tuple[Path, bool]:
    if input_name.lower().endswith(".zip"):
        if output_path is None:
            base_dir = Path.cwd()
        else:
            base_dir = output_path if output_path.suffix.lower() != ".xlsx" else output_path.parent
        target_dir = base_dir / invoice_ref
        target_dir.mkdir(parents=True, exist_ok=True)

        excel_path = target_dir / f"{invoice_ref}.xlsx"
        skipped_existing = False
        if excel_path.exists():
            skipped_existing = True
        else:
            export_price_rows(price_rows, excel_path, sheet_name=sheet_name, header=header, config=config)

        if pdf_bytes and pdf_name:
            pdf_path = target_dir / _safe_name(Path(pdf_name).name)
            if not pdf_path.exists():
                pdf_path.write_bytes(pdf_bytes)

        return target_dir, skipped_existing

    if output_path is None:
        raise ValueError("output_path es obligatorio para XML directo.")

    if output_path.suffix.lower() == ".xlsx":
        if output_path.exists():
            return output_path, True
        export_price_rows(price_rows, output_path, sheet_name=sheet_name, header=header, config=config)
        return output_path, False

    target_dir = output_path / invoice_ref
    target_dir.mkdir(parents=True, exist_ok=True)
    excel_path = target_dir / f"{invoice_ref}.xlsx"
    if excel_path.exists():
        return target_dir, True
    export_price_rows(price_rows, excel_path, sheet_name=sheet_name, header=header, config=config)
    return target_dir, False


def process_invoice_bytes(
    input_name: str,
    input_bytes: bytes,
    output_path: Optional[Path],
    config: MarkupConfig,
    sheet_name: str = "Productos",
    rules_path: Optional[Path] = None,
    generate_output: bool = True,
) -> ProcessResult:
    parse_started = time.perf_counter()
    invoice_root, xml_name, pdf_name, pdf_bytes, raw_xml_bytes = load_invoice_root_bytes(input_name, input_bytes)
    invoice_lines = parse_invoice_lines(invoice_root)
    invoice_header = parse_invoice_header(invoice_root)
    parse_ms = (time.perf_counter() - parse_started) * 1000

    pricing_started = time.perf_counter()
    rules_file = rules_path or _default_rules_path()
    rules = load_rules(rules_file) if rules_file else []
    price_rows = build_price_rows(invoice_lines, config, rules=rules)
    pricing_ms = (time.perf_counter() - pricing_started) * 1000

    invoice_ref = _safe_name(invoice_header.invoice_id) if invoice_header.invoice_id else _safe_name(Path(xml_name).stem)
    metrics = ProcessMetrics(parse_ms=parse_ms, pricing_ms=pricing_ms)

    result = ProcessResult(
        output_path=None,
        skipped_existing=False,
        header=invoice_header,
        price_rows=price_rows,
        pdf_bytes=pdf_bytes,
        pdf_filename=pdf_name,
        raw_xml=raw_xml_bytes.decode("utf-8", errors="replace") if raw_xml_bytes else None,
        invoice_ref=invoice_ref,
        metrics=metrics,
    )

    if not generate_output:
        return result

    artifact_started = time.perf_counter()
    resolved_output_path, skipped_existing = _write_invoice_artifacts(
        input_name=input_name,
        output_path=output_path,
        sheet_name=sheet_name,
        header=invoice_header,
        price_rows=price_rows,
        config=config,
        pdf_bytes=pdf_bytes,
        pdf_name=pdf_name,
        invoice_ref=invoice_ref,
    )
    result.output_path = resolved_output_path
    result.skipped_existing = skipped_existing
    result.metrics.artifact_ms = (time.perf_counter() - artifact_started) * 1000
    return result


def process_invoice(
    input_path: Path,
    output_path: Optional[Path],
    config: MarkupConfig,
    sheet_name: str = "Productos",
    rules_path: Optional[Path] = None,
) -> ProcessResult:
    resolved_output = output_path
    if input_path.suffix.lower() != ".zip" and resolved_output is None:
        resolved_output = input_path.with_suffix(".xlsx")

    if input_path.suffix.lower() == ".zip" and resolved_output is None:
        resolved_output = input_path.parent

    return process_invoice_bytes(
        input_name=input_path.name,
        input_bytes=input_path.read_bytes(),
        output_path=resolved_output,
        config=config,
        sheet_name=sheet_name,
        rules_path=rules_path,
        generate_output=True,
    )
