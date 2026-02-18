import argparse
from decimal import Decimal
from pathlib import Path
from typing import Optional, Sequence

from .processor import process_invoice
from .pricing import MarkupConfig

# Directorio de facturas (carpeta invoices en la raiz del proyecto).
DEFAULT_INVOICE_DIR = Path(__file__).resolve().parents[2] / "invoices"


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"No es un numero valido: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convierte una factura XML (AttachedDocument UBL) a Excel con precios de costo/venta."
    )
    parser.add_argument("--input", "-i", required=True, help="Ruta al XML de factura (AttachedDocument).")
    parser.add_argument(
        "--output",
        "-o",
        required=False,
        help="Ruta de salida. Para ZIP es una carpeta base; para XML puede ser .xlsx o carpeta. Si no se indica, se usa el nombre del XML.",
    )
    parser.add_argument(
        "--markup-threshold",
        type=_decimal_arg,
        default=Decimal("10000"),
        help="Umbral en costo neto para decidir la utilidad (default: 10000).",
    )
    parser.add_argument(
        "--markup-below",
        type=_decimal_arg,
        default=Decimal("0.68"),
        help="Divisor para calcular venta bruta cuando el costo neto es menor al umbral (default: 0.68).",
    )
    parser.add_argument(
        "--markup-above",
        type=_decimal_arg,
        default=Decimal("1.32"),
        help="Multiplicador para calcular venta bruta cuando el costo neto es mayor o igual al umbral (default: 1.32).",
    )
    parser.add_argument("--sheet", default="Productos", help="Nombre de la hoja en el Excel.")
    parser.add_argument(
        "--rules",
        help="Ruta a un archivo XLSX con reglas especiales de utilidad.",
    )
    parser.add_argument(
        "--round-net-step",
        type=_decimal_arg,
        default=Decimal("100"),
        help="Paso de redondeo para el valor de venta neto (default: 100).",
    )
    parser.add_argument(
        "--rounding-mode",
        choices=["nearest", "up", "down"],
        default="up",
        help="Modo de redondeo para el valor de venta neto (default: up, evita miles cerrados).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    DEFAULT_INVOICE_DIR.mkdir(exist_ok=True)

    input_path = Path(args.input)
    if not input_path.exists():
        alt = DEFAULT_INVOICE_DIR / args.input
        if alt.exists():
            input_path = alt
        else:
            raise FileNotFoundError(f"No se encontró el archivo {args.input} ni en {DEFAULT_INVOICE_DIR}")

    config = MarkupConfig(
        threshold=args.markup_threshold,
        below_divisor=args.markup_below,
        above_multiplier=args.markup_above,
        round_net_step=args.round_net_step,
        rounding_mode=args.rounding_mode,
    )

    output_path = Path(args.output) if args.output else None
    rules_path = Path(args.rules) if args.rules else None
    result = process_invoice(input_path, output_path, config, sheet_name=args.sheet, rules_path=rules_path)
    if result.skipped_existing:
        print(f"No se sobrescribio, archivo existente: {result.output_path}")
    else:
        print(f"Generado {result.output_path}")


if __name__ == "__main__":
    main()
