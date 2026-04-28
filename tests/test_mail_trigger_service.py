import io
import os
import sys
import unittest
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from facturador.mail_trigger_service import create_app
from facturador.mail_automation import MailAutomationService
from facturador.pricing import MarkupConfig
from facturador.processor import process_invoice_bytes


class FakeMailService:
    def __init__(self, result=None, error=None):
        self.result = result or {
            "documentKind": "PURCHASE_INVOICE",
            "invoiceId": "invoice-1",
            "matchPercentage": 100,
            "isNew": True,
            "statusCode": 201,
        }
        self.error = error
        self.calls = []

    def process_uploaded_zip(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


class ProcessZipEndpointTest(unittest.TestCase):
    def build_client(self):
        os.environ["FACTURADOR_ADMIN_TOKEN"] = "secret"
        app = create_app()
        return app.test_client()

    def test_requires_admin_token(self):
        client = self.build_client()

        response = client.post(
            "/admin/process-zip",
            data={"file": (io.BytesIO(b"zip-data"), "factura.zip")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 401)

    def test_rejects_non_zip_file(self):
        client = self.build_client()

        response = client.post(
            "/admin/process-zip",
            data={"file": (io.BytesIO(b"xml"), "factura.xml")},
            headers={"X-Facturador-Admin-Token": "secret"},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "invalid_file_type")

    def test_returns_zip_processing_errors(self):
        fake_mail = FakeMailService(error=FileNotFoundError("No se encontro ningun XML dentro del ZIP."))
        client = self.build_client()

        with patch(
            "facturador.mail_trigger_service.GmailPushProcessor._ensure_mail",
            return_value=fake_mail,
        ):
            response = client.post(
                "/admin/process-zip",
                data={"file": (io.BytesIO(b"not-a-real-zip"), "factura.zip")},
                headers={"X-Facturador-Admin-Token": "secret"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "zip_processing_failed")

    def test_processes_zip_with_mail_service(self):
        with patch(
            "facturador.mail_automation.MailAutomationService._build_google_services",
            return_value=(None, None, None),
        ), patch(
            "facturador.mail_automation.MailAutomationService._ensure_gmail_label",
            return_value="label-id",
        ), patch(
            "facturador.mail_automation.MailAutomationService.process_uploaded_zip",
            return_value={
                "documentKind": "PURCHASE_INVOICE",
                "invoiceId": "invoice-1",
                "matchPercentage": 100,
                "isNew": True,
                "statusCode": 201,
            },
        ) as process_uploaded_zip:
            client = self.build_client()
            response = client.post(
                "/admin/process-zip?skip_drive=1",
                data={"file": (io.BytesIO(b"zip-data"), "factura.zip")},
                headers={"X-Facturador-Admin-Token": "secret"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["invoiceId"], "invoice-1")
        call = process_uploaded_zip.call_args.kwargs
        self.assertEqual(call["attachment_name"], "factura.zip")
        self.assertEqual(call["data"], b"zip-data")


class ManualZipPayloadTest(unittest.TestCase):
    def test_processes_local_zip_and_builds_erp_payload(self):
        xml_path = next((ROOT / "invoices").glob("*.xml"))
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.write(xml_path, arcname=xml_path.name)

        result = process_invoice_bytes(
            input_name="factura-dian.zip",
            input_bytes=zip_buffer.getvalue(),
            output_path=None,
            config=MarkupConfig(
                threshold=Decimal("10000"),
                below_divisor=Decimal("0.68"),
                above_multiplier=Decimal("1.32"),
            ),
            generate_output=False,
        )
        service = MailAutomationService.__new__(MailAutomationService)
        payload = service._build_erp_payload(result)

        self.assertEqual(payload["documentKind"], "PURCHASE_INVOICE")
        self.assertTrue(payload["invoice"]["invoiceNumber"])
        self.assertTrue(payload["supplier"]["nit"])
        self.assertGreater(len(payload["lines"]), 0)
        self.assertIn("suggestedPriceIncTax", payload["lines"][0])


if __name__ == "__main__":
    unittest.main()
