from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, ROUND_FLOOR
from typing import List, Optional

from .invoice_parser import InvoiceLine
from .rules import PricingRule, find_rule


@dataclass
class MarkupConfig:
    threshold: Decimal = Decimal("10000")
    below_divisor: Decimal = Decimal("0.68")
    above_multiplier: Decimal = Decimal("1.32")
    round_net_step: Decimal = Decimal("100")
    rounding_mode: str = "up"  # up|nearest|down


@dataclass
class PriceRow:
    product: str
    quantity: Decimal
    tax_percent: Decimal
    discount_percent: Decimal
    cost_bruto_unit: Decimal
    cost_neto_unit: Decimal
    venta_bruta_unit: Decimal
    venta_neta_unit: Decimal
    source_line_id: str
    markup_percent: Optional[Decimal] = None


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _round_to_step(value: Decimal, step: Decimal, mode: str) -> Decimal:
    """
    Redondea el valor al multiplo mas cercano de `step` segun el modo indicado.
    """
    if step <= 0:
        return value
    step = abs(step)
    ratio = value / step
    if mode == "nearest":
        rounded = ratio.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    elif mode == "up":
        rounded = ratio.to_integral_value(rounding=ROUND_CEILING)
    elif mode == "down":
        rounded = ratio.to_integral_value(rounding=ROUND_FLOOR)
    else:
        raise ValueError(f"Rounding mode no soportado: {mode}")
    result = rounded * step
    # Evitar cifras cerradas en miles (ej: 16000); si cae exacto, subir un paso.
    if result % Decimal("1000") == 0:
        result += step
    return result


def build_price_rows(
    lines: List[InvoiceLine],
    config: MarkupConfig,
    rules: Optional[List[PricingRule]] = None,
) -> List[PriceRow]:
    rows: List[PriceRow] = []
    for line in lines:
        qty = line.quantity
        tax_factor = (line.tax_percent or Decimal("0")) / Decimal("100")
        base_unit = Decimal("0")
        if qty == 0:
            cost_bruto_unit = Decimal("0")
        else:
            # Precio base por unidad (antes de descuento)
            base_unit = line.base_amount / qty
            cost_bruto_unit = base_unit
        discount_factor = Decimal("1") - (line.discount_percent / Decimal("100"))
        if discount_factor < 0:
            discount_factor = Decimal("0")

        # Costo neto incluye descuento e IVA
        cost_neto_unit = base_unit * discount_factor * (Decimal("1") + tax_factor)

        # Utilidad sobre costo neto => valor de venta neto; luego quitar IVA para venta bruta.
        rule = find_rule(line.description, rules or [])
        if rule and rule.utilidad_percent is not None:
            venta_neta_unit_raw = cost_neto_unit * (Decimal("1") + rule.utilidad_percent / Decimal("100"))
        else:
            venta_neta_unit_raw = (
                cost_neto_unit / config.below_divisor
                if cost_neto_unit < config.threshold
                else cost_neto_unit * config.above_multiplier
            )
        venta_neta_unit = _round_to_step(venta_neta_unit_raw, config.round_net_step, config.rounding_mode)
        venta_bruta_unit = venta_neta_unit / (Decimal("1") + tax_factor)

        rows.append(
            PriceRow(
                product=line.description,
                quantity=line.quantity,
                tax_percent=line.tax_percent,
                discount_percent=line.discount_percent,
                cost_bruto_unit=_money(cost_bruto_unit),
                cost_neto_unit=_money(cost_neto_unit),
                venta_neta_unit=_money(venta_neta_unit),
                venta_bruta_unit=_money(venta_bruta_unit),
                source_line_id=line.line_id,
                markup_percent=rule.utilidad_percent if rule else None,
            )
        )
    return rows
