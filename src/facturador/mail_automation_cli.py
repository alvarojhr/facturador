import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

from .mail_automation import (
    MailAutomationService,
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

    service = MailAutomationService(cfg)
    if args.once:
        summary = service.run_once()
        print(
            "Ciclo completado: "
            f"mensajes={summary.checked_messages}, "
            f"procesados={summary.processed_messages}, "
            f"adjuntos={summary.processed_attachments}, "
            f"fallidos={summary.failed_messages}"
        )
        return

    print("Automatizacion iniciada. Presiona Ctrl+C para detener.")
    try:
        service.run_forever()
    except KeyboardInterrupt:
        print("\nAutomatizacion detenida por usuario.")


if __name__ == "__main__":
    main()
