from dataclasses import dataclass
from decimal import Decimal
import xml.etree.ElementTree as ET
from typing import List


OUTER_NS = {
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
}

INVOICE_NS = {
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
}


@dataclass
class InvoiceLine:
    line_id: str
    description: str
    quantity: Decimal
    line_extension_amount: Decimal
    tax_percent: Decimal
    base_amount: Decimal
    discount_percent: Decimal


@dataclass
class InvoiceHeader:
    supplier_name: str
    supplier_id: str
    customer_name: str
    invoice_id: str
    cufe: str
    issue_date: str
    due_date: str
    currency: str
    subtotal: Decimal
    tax_total: Decimal
    total: Decimal
    total_tax_inclusive: Decimal


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value)
    except Exception:
        return Decimal(default)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _is_invoice_root(root: ET.Element) -> bool:
    return _local_name(root.tag) == "Invoice"


def _extract_invoice_root_from_attached(root: ET.Element) -> ET.Element:
    for desc in root.findall(".//cac:Attachment/cac:ExternalReference/cbc:Description", OUTER_NS):
        payload = desc.text
        if not payload or "<Invoice" not in payload:
            continue
        try:
            return ET.fromstring(payload)
        except ET.ParseError:
            continue
    raise ValueError("No se encontro un XML de Invoice embebido en el archivo.")


def extract_invoice_root_from_bytes(data: bytes) -> ET.Element:
    root = ET.fromstring(data)
    if _is_invoice_root(root):
        return root
    return _extract_invoice_root_from_attached(root)


def extract_invoice_root(path: str) -> ET.Element:
    """
    Busca el primer bloque <Invoice> embebido en los cbc:Description del AttachedDocument.
    """
    with open(path, "rb") as handle:
        data = handle.read()
    return extract_invoice_root_from_bytes(data)


def parse_invoice_lines(invoice_root: ET.Element) -> List[InvoiceLine]:
    """
    Convierte las cac:InvoiceLine en objetos InvoiceLine con decimales.
    """
    lines: List[InvoiceLine] = []
    for node in invoice_root.findall(".//cac:InvoiceLine", INVOICE_NS):
        line_id = (node.findtext("cbc:ID", namespaces=INVOICE_NS) or "").strip()
        description = (node.findtext("cac:Item/cbc:Description", namespaces=INVOICE_NS) or "").strip()
        quantity = _to_decimal(node.findtext("cbc:InvoicedQuantity", namespaces=INVOICE_NS) or "0")
        line_extension = _to_decimal(node.findtext("cbc:LineExtensionAmount", namespaces=INVOICE_NS) or "0")
        tax_percent = _to_decimal(
            node.findtext(".//cac:TaxCategory/cbc:Percent", namespaces=INVOICE_NS) or "0"
        )

        # Descuentos: sumamos los Multipliers y los Amount de AllowanceCharge con ChargeIndicator = false
        discount_percent = Decimal("0")
        allowance_total = Decimal("0")
        base_amount_candidates: List[Decimal] = []
        multiplier_sum = Decimal("0")
        for allowance in node.findall("cac:AllowanceCharge", INVOICE_NS):
            charge_indicator = allowance.findtext("cbc:ChargeIndicator", namespaces=INVOICE_NS) or ""
            if charge_indicator.lower() == "true":
                continue
            disc_pct = allowance.findtext("cbc:MultiplierFactorNumeric", namespaces=INVOICE_NS)
            if disc_pct:
                multiplier_sum += _to_decimal(disc_pct)
            amount = allowance.findtext("cbc:Amount", namespaces=INVOICE_NS)
            if amount:
                allowance_total += _to_decimal(amount)
            base = allowance.findtext("cbc:BaseAmount", namespaces=INVOICE_NS)
            if base:
                base_amount_candidates.append(_to_decimal(base))

        base_amount_raw = base_amount_candidates[0] if base_amount_candidates else None
        if base_amount_raw is not None:
            base_amount = base_amount_raw
            if quantity > 0 and base_amount_raw < line_extension and base_amount_raw * quantity >= line_extension:
                base_amount = base_amount_raw * quantity
        elif multiplier_sum > 0 and line_extension > 0 and multiplier_sum < Decimal("100"):
            base_amount = line_extension / (Decimal("1") - (multiplier_sum / Decimal("100")))
        else:
            base_amount = line_extension + allowance_total if allowance_total else line_extension

        # Calcular descuento efectivo:
        # 1) Si base >= line_extension, inferir desde base-line_extension.
        # 2) Si solo hay multiplicador, usarlo.
        # 3) Si hay monto de descuento, usar monto/base.
        if base_amount > 0 and line_extension > 0 and line_extension <= base_amount:
            discount_percent = (Decimal("1") - (line_extension / base_amount)) * Decimal("100")
        elif multiplier_sum > 0:
            discount_percent = multiplier_sum
        elif base_amount > 0 and allowance_total > 0:
            discount_percent = (allowance_total / base_amount) * Decimal("100")

        if discount_percent < 0:
            discount_percent = Decimal("0")

        lines.append(
            InvoiceLine(
                line_id=line_id,
                description=description,
                quantity=quantity,
                line_extension_amount=line_extension,
                tax_percent=tax_percent,
                base_amount=base_amount,
                discount_percent=discount_percent,
            )
        )
    if not lines:
        raise ValueError("La Invoice no contiene lineas de items.")
    return lines


def _first_text(node: ET.Element, paths: List[str]) -> str:
    for path in paths:
        value = node.findtext(path, namespaces=INVOICE_NS)
        if value:
            return value.strip()
    return ""


def parse_invoice_header(invoice_root: ET.Element) -> InvoiceHeader:
    supplier_name = _first_text(
        invoice_root,
        [
            "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName",
            "cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName",
            "cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name",
        ],
    )
    supplier_id = _first_text(
        invoice_root,
        [
            "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID",
            "cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:CompanyID",
        ],
    )
    customer_name = _first_text(
        invoice_root,
        [
            "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName",
            "cac:AccountingCustomerParty/cac:Party/cac:PartyName/cbc:Name",
        ],
    )
    invoice_id = _first_text(invoice_root, ["cbc:ID"])
    cufe = _first_text(invoice_root, ["cbc:UUID"])
    issue_date = _first_text(invoice_root, ["cbc:IssueDate"])
    due_date = _first_text(invoice_root, ["cbc:DueDate"])
    currency = _first_text(invoice_root, ["cbc:DocumentCurrencyCode"])

    subtotal = _to_decimal(
        invoice_root.findtext("cac:LegalMonetaryTotal/cbc:LineExtensionAmount", namespaces=INVOICE_NS) or "0"
    )
    total_tax_inclusive = _to_decimal(
        invoice_root.findtext("cac:LegalMonetaryTotal/cbc:TaxInclusiveAmount", namespaces=INVOICE_NS) or "0"
    )
    total = _to_decimal(
        invoice_root.findtext("cac:LegalMonetaryTotal/cbc:PayableAmount", namespaces=INVOICE_NS) or "0"
    )
    tax_total = Decimal("0")
    for tax in invoice_root.findall("cac:TaxTotal", INVOICE_NS):
        tax_total += _to_decimal(tax.findtext("cbc:TaxAmount", namespaces=INVOICE_NS) or "0")

    return InvoiceHeader(
        supplier_name=supplier_name,
        supplier_id=supplier_id,
        customer_name=customer_name,
        invoice_id=invoice_id,
        cufe=cufe,
        issue_date=issue_date,
        due_date=due_date,
        currency=currency,
        subtotal=subtotal,
        tax_total=tax_total,
        total=total,
        total_tax_inclusive=total_tax_inclusive,
    )
