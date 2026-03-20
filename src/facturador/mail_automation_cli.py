import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

from .mail_automation import (
    MailAutomationService,
    RuntimeOptions,
    default_mail_automation_config_path,
    load_mail_automation_config,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automatiza Facturador leyendo ZIPs desde Gmail y subiendo resultados a Google Drive."
    )
    parser.add_argument(
        "--config",
        required=False,
        help="Ruta a mail_automation.json. Si no se indica, usa config/mail_automation.json.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Ejecuta un solo ciclo y termina.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        help="Sobrescribe poll_interval_sec (segundos) en esta ejecucion.",
    )
    parser.add_argument(
        "--max-messages-per-poll",
        type=int,
        help="Sobrescribe max_messages_per_poll en esta ejecucion. Si no se indica, procesa sin limite.",
    )
    parser.add_argument(
        "--skip-drive",
        action="store_true",
        help="Procesa hacia el ERP sin crear carpetas ni subir archivos a Google Drive.",
    )
    parser.add_argument(
        "--skip-ingresado-sync",
        action="store_true",
        help="Omite la sincronizacion del label 'Ingresado' en esta ejecucion.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        help="Cantidad de workers para --once. Default: 4.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Activa logs detallados.",
    )
    parser.add_argument(
        "--log-file",
        required=False,
        help="Ruta a archivo de log para ejecucion continua.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if args.log_file:
        log_path = Path(args.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    log_level = logging.INFO if args.verbose or args.log_file else logging.WARNING
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)

    config_path = Path(args.config) if args.config else default_mail_automation_config_path()
    cfg = load_mail_automation_config(config_path)
    if args.poll_interval is not None:
        cfg.poll_interval_sec = args.poll_interval
    if args.max_messages_per_poll is not None:
        if args.max_messages_per_poll < 1:
            raise SystemExit("--max-messages-per-poll debe ser >= 1.")
        cfg.max_messages_per_poll = args.max_messages_per_poll
    else:
        cfg.max_messages_per_poll = None

    service = MailAutomationService(cfg)
    runtime = RuntimeOptions(
        skip_drive=args.skip_drive,
        skip_ingresado_sync=args.skip_ingresado_sync,
        concurrency=args.concurrency if args.concurrency is not None else (4 if args.once else 1),
    )
    if args.once:
        summary = service.run_once(runtime=runtime)
        print(
            "Ciclo completado: "
            f"mensajes={summary.checked_messages}, "
            f"procesados={summary.processed_messages}, "
            f"adjuntos={summary.processed_attachments}, "
            f"fallidos={summary.failed_messages}, "
            f"bytes={summary.bytes_processed}, "
            f"gmail_list_ms={summary.gmail_list_ms:.1f}, "
            f"gmail_download_ms={summary.gmail_download_ms:.1f}, "
            f"parse_ms={summary.parse_ms:.1f}, "
            f"pricing_ms={summary.pricing_ms:.1f}, "
            f"erp_ms={summary.erp_ms:.1f}, "
            f"drive_ms={summary.drive_ms:.1f}, "
            f"gcs_ms={summary.gcs_ms:.1f}, "
            f"label_ms={summary.label_ms:.1f}"
        )
        return

    print("Automatizacion iniciada. Presiona Ctrl+C para detener.")
    try:
        service.run_forever()
    except KeyboardInterrupt:
        print("\nAutomatizacion detenida por usuario.")


if __name__ == "__main__":
    main()
