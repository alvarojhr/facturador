import os
import sys
from decimal import Decimal
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Optional

from . import __version__
from .pricing import MarkupConfig
from .processor import process_invoice
from .updater import (
    UpdateInfo,
    check_for_update,
    download_update_installer,
    launch_installer,
    load_update_config,
)


class FacturadorApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        root.title("Facturador XML a Excel")
        root.geometry("720x420")

        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.threshold_var = tk.StringVar(value="10000")
        self.below_var = tk.StringVar(value="0.68")
        self.above_var = tk.StringVar(value="1.32")
        self.round_step_var = tk.StringVar(value="100")
        self.round_mode_var = tk.StringVar(value="up")
        self.sheet_var = tk.StringVar(value="Productos")
        self.rules_var = tk.StringVar(value=str(self._default_rules_path()))
        self.status_var = tk.StringVar(value="Listo.")
        self.last_result_path: Optional[Path] = None
        self.update_cfg = load_update_config()

        self._build_ui()
        if self.update_cfg.check_on_startup:
            self.root.after(1200, lambda: self._check_updates(manual=False))

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        title = ttk.Label(main, text="Facturador XML / ZIP a Excel", font=("Segoe UI", 14, "bold"))
        title.pack(anchor="w")

        input_frame = ttk.LabelFrame(main, text="Archivo de entrada")
        input_frame.pack(fill=tk.X, pady=8)
        input_entry = ttk.Entry(input_frame, textvariable=self.input_var)
        input_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6, pady=6)
        ttk.Button(input_frame, text="Buscar", command=self._browse_input).pack(side=tk.LEFT, padx=6, pady=6)

        output_frame = ttk.LabelFrame(main, text="Salida (carpeta opcional)")
        output_frame.pack(fill=tk.X, pady=4)
        output_entry = ttk.Entry(output_frame, textvariable=self.output_var)
        output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6, pady=6)
        ttk.Button(output_frame, text="Elegir carpeta", command=self._browse_output).pack(side=tk.LEFT, padx=6, pady=6)

        options = ttk.LabelFrame(main, text="Opciones")
        options.pack(fill=tk.X, pady=8)

        self._add_option_row(options, "Umbral costo neto", self.threshold_var, 0, 0)
        self._add_option_row(options, "Divisor < umbral", self.below_var, 0, 1)
        self._add_option_row(options, "Multiplicador >= umbral", self.above_var, 0, 2)
        self._add_option_row(options, "Paso redondeo neto", self.round_step_var, 1, 0)
        self._add_option_row(options, "Nombre hoja", self.sheet_var, 1, 1)

        ttk.Label(options, text="Modo redondeo").grid(row=1, column=2, padx=6, pady=6, sticky="w")
        mode = ttk.Combobox(options, textvariable=self.round_mode_var, values=["up", "nearest", "down"], width=12)
        mode.grid(row=1, column=3, padx=6, pady=6, sticky="w")
        mode.state(["readonly"])

        rules_frame = ttk.LabelFrame(main, text="Reglas especiales (XLSX opcional)")
        rules_frame.pack(fill=tk.X, pady=4)
        ttk.Entry(rules_frame, textvariable=self.rules_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6, pady=6)
        ttk.Button(rules_frame, text="Buscar", command=self._browse_rules).pack(side=tk.LEFT, padx=6, pady=6)

        actions = ttk.Frame(main)
        actions.pack(fill=tk.X, pady=8)
        ttk.Button(actions, text="Procesar", command=self._process).pack(side=tk.LEFT)
        ttk.Button(actions, text="Abrir carpeta salida", command=self._open_output_folder).pack(side=tk.LEFT, padx=8)
        ttk.Button(actions, text="Buscar actualizaciones", command=lambda: self._check_updates(manual=True)).pack(
            side=tk.LEFT, padx=8
        )

        status = ttk.LabelFrame(main, text="Estado")
        status.pack(fill=tk.BOTH, expand=True, pady=8)
        ttk.Label(status, textvariable=self.status_var).pack(anchor="w", padx=6, pady=6)

    def _add_option_row(self, parent: ttk.LabelFrame, label: str, var: tk.StringVar, row: int, col: int) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=col * 2, padx=6, pady=6, sticky="w")
        ttk.Entry(parent, textvariable=var, width=16).grid(row=row, column=col * 2 + 1, padx=6, pady=6, sticky="w")

    def _browse_input(self) -> None:
        path = filedialog.askopenfilename(
            title="Seleccionar ZIP o XML",
            filetypes=[("ZIP/XML", "*.zip;*.xml"), ("ZIP", "*.zip"), ("XML", "*.xml")],
        )
        if path:
            self.input_var.set(path)

    def _browse_output(self) -> None:
        path = filedialog.askdirectory(title="Seleccionar carpeta de salida")
        if path:
            self.output_var.set(path)

    def _browse_rules(self) -> None:
        path = filedialog.askopenfilename(
            title="Seleccionar reglas XLSX",
            filetypes=[("Excel", "*.xlsx")],
        )
        if path:
            self.rules_var.set(path)

    def _default_rules_path(self) -> Path:
        if getattr(sys, "frozen", False):
            base = Path(sys.executable).resolve().parent
        else:
            base = Path(__file__).resolve().parents[2]
        return base / "config" / "reglas_especiales.xlsx"

    def _make_config(self) -> MarkupConfig:
        return MarkupConfig(
            threshold=Decimal(self.threshold_var.get()),
            below_divisor=Decimal(self.below_var.get()),
            above_multiplier=Decimal(self.above_var.get()),
            round_net_step=Decimal(self.round_step_var.get()),
            rounding_mode=self.round_mode_var.get(),
        )

    def _process(self) -> None:
        input_path = self.input_var.get().strip()
        if not input_path:
            messagebox.showerror("Error", "Selecciona un archivo ZIP o XML.")
            return
        input_path = Path(input_path)
        if not input_path.exists():
            messagebox.showerror("Error", "El archivo seleccionado no existe.")
            return

        output_path = self.output_var.get().strip()
        output_path = Path(output_path) if output_path else None

        try:
            config = self._make_config()
            sheet = self.sheet_var.get().strip() or "Productos"
            rules_path = Path(self.rules_var.get().strip()) if self.rules_var.get().strip() else None
            result = process_invoice(input_path, output_path, config, sheet_name=sheet, rules_path=rules_path)
        except Exception as exc:
            self.status_var.set(f"Error: {exc}")
            messagebox.showerror("Error", str(exc))
            return

        self.last_result_path = result.output_path
        if result.skipped_existing:
            self.status_var.set(f"No se sobrescribio, ya existia: {result.output_path}")
        else:
            self.status_var.set(f"Generado: {result.output_path}")

        target = result.output_path if result.output_path.is_dir() else result.output_path.parent
        try:
            os.startfile(str(target))
        except Exception:
            pass

    def _open_output_folder(self) -> None:
        if self.last_result_path is not None:
            out_path = self.last_result_path
        else:
            path = self.output_var.get().strip()
            if not path:
                messagebox.showinfo("Info", "No hay salida disponible.")
                return
            out_path = Path(path)
        if out_path.exists():
            target = out_path if out_path.is_dir() else out_path.parent
            os.startfile(str(target))
        else:
            messagebox.showinfo("Info", "El archivo de salida no existe aun.")

    def _check_updates(self, manual: bool) -> None:
        if not self.update_cfg.manifest_url:
            if manual:
                messagebox.showinfo(
                    "Actualizaciones",
                    "Configura 'manifest_url' en config/update_config.json para buscar actualizaciones.",
                )
            return

        def worker() -> None:
            try:
                info = check_for_update(__version__, self.update_cfg)
            except Exception as exc:
                if manual:
                    self.root.after(0, lambda: messagebox.showerror("Actualizaciones", str(exc)))
                return

            if not info:
                if manual:
                    self.root.after(0, lambda: messagebox.showinfo("Actualizaciones", "Ya tienes la version mas reciente."))
                return
            self.root.after(0, lambda: self._prompt_update(info))

        threading.Thread(target=worker, daemon=True).start()

    def _prompt_update(self, info: UpdateInfo) -> None:
        details = f"Version actual: {__version__}\nNueva version: {info.version}"
        if info.notes:
            details += f"\n\nNotas:\n{info.notes}"
        if info.mandatory:
            details += "\n\nEsta actualizacion es obligatoria."

        if not self.update_cfg.auto_install and not info.mandatory:
            proceed = messagebox.askyesno("Actualizacion disponible", f"{details}\n\nDeseas actualizar ahora?")
            if not proceed:
                return
        elif not self.update_cfg.auto_install and info.mandatory:
            messagebox.showinfo("Actualizacion obligatoria", details)

        self.status_var.set("Descargando actualizacion...")

        def installer_worker() -> None:
            try:
                installer = download_update_installer(info)
                launch_installer(installer, self.update_cfg.silent_install_args)
            except Exception as exc:
                self.root.after(0, lambda: messagebox.showerror("Actualizaciones", str(exc)))
                self.root.after(0, lambda: self.status_var.set("Error en actualizacion."))
                return

            def finish() -> None:
                messagebox.showinfo("Actualizaciones", "Se iniciara el instalador de actualizacion.")
                self.root.destroy()

            self.root.after(0, finish)

        threading.Thread(target=installer_worker, daemon=True).start()


def main() -> None:
    root = tk.Tk()
    FacturadorApp(root)
    root.mainloop()
