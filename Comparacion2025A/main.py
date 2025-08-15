import os
import re
import sys
import hashlib
import unicodedata
from typing import Dict, List, Union

import pandas as pd


# =========================
# Utilidades de nombres
# =========================
def remove_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def to_snake_case(s: str) -> str:
    s = re.sub(r"[^\w\s-]", " ", s, flags=re.UNICODE)
    s = re.sub(r"[-\s]+", "_", s.strip().lower())
    s = re.sub(r"_+", "_", s)
    return s

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza encabezados: quita acentos, snake_case y resuelve duplicados."""
    new_cols = []
    seen = {}
    for c in df.columns:
        name = to_snake_case(remove_accents(str(c).strip()))
        if not name:
            name = "columna"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        new_cols.append(name)
    out = df.copy()
    out.columns = new_cols
    return out

def safe_filename(name: str, max_len: int = 100) -> str:
    """
    Devuelve un nombre de archivo seguro y corto para Windows:
    - normaliza (acentos -> ASCII, snake_case)
    - elimina caracteres inválidos
    - si es largo, trunca y agrega hash
    """
    base = to_snake_case(remove_accents(str(name)))
    base = re.sub(r'[\\/:*?"<>|]+', "_", base).strip(" .")
    if not base:
        base = "columna"
    if len(base) > max_len:
        h = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
        base = f"{base[:max_len-9]}_{h}"
    return base


# =========================
# Carga Excel (todas las hojas) y unificación
# =========================
def read_excel_all_sheets(path: str, sheets_cfg: Union[str, List[str]] = "all") -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path)  # requiere openpyxl
    if sheets_cfg == "all":
        sheets = xls.sheet_names
    elif isinstance(sheets_cfg, list):
        sheets = sheets_cfg
    else:
        raise ValueError("sheets_cfg debe ser 'all' o lista de nombres de hoja.")
    frames = {sh: pd.read_excel(path, sheet_name=sh, dtype="object") for sh in sheets}
    return frames

def unify_sheets(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Une todas las hojas por columnas; agrega '_sheet' para trazabilidad."""
    # Normaliza encabezados por hoja
    frames_norm = {sh: normalize_headers(df) for sh, df in frames.items()}
    # Alinea columnas
    all_cols = set()
    for df in frames_norm.values():
        all_cols.update(df.columns.tolist())
    all_cols = list(all_cols)

    aligned = []
    for sh, df in frames_norm.items():
        tmp = df.copy()
        for c in all_cols:
            if c not in tmp.columns:
                tmp[c] = pd.NA
        tmp = tmp[all_cols]
        tmp["_sheet"] = sh
        aligned.append(tmp)
    return pd.concat(aligned, ignore_index=True)


# =========================
# Tipado SIMPLE (numérico vs string)
# =========================
def build_simple_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    """
    Regla simple por columna:
    - si TODOS los valores no nulos son numéricos -> int si todos enteros; si no -> float
    - caso contrario -> string
    """
    overrides = {}
    for c in df.columns:
        if c == "_sheet":
            overrides[c] = "string"
            continue
        s = df[c]
        if s.dropna().empty:
            overrides[c] = "string"
            continue
        nums = pd.to_numeric(s, errors="coerce")
        mask = s.notna()
        all_numeric = nums[mask].notna().all()
        if all_numeric:
            overrides[c] = "int" if (nums[mask] % 1 == 0).all() else "float"
        else:
            overrides[c] = "string"
    return overrides

def apply_dtypes(df: pd.DataFrame, dtypes: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for col, t in dtypes.items():
        if col not in out.columns:
            continue
        try:
            if t == "string":
                out[col] = out[col].astype("string")
            elif t == "int":
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif t == "float":
                out[col] = pd.to_numeric(out[col], errors="coerce")
            else:
                # fallback a string si llega algo no esperado
                out[col] = out[col].astype("string")
        except Exception:
            out[col] = out[col].astype("string")
    return out


# =========================
# Exportaciones
# =========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def export_master_csv(df: pd.DataFrame, outdir: str):
    ensure_dir(outdir)
    df.to_csv(os.path.join(outdir, "master.csv"), index=False, encoding="utf-8-sig")

def export_columns_csvs(df: pd.DataFrame, outdir: str):
    col_dir = os.path.join(outdir, "columns")
    ensure_dir(col_dir)
    used = set()
    for col in df.columns:
        fname = safe_filename(col)
        candidate = fname
        i = 1
        while candidate in used or os.path.exists(os.path.join(col_dir, f"{candidate}.csv")):
            i += 1
            candidate = f"{fname}_{i}"
        used.add(candidate)
        path = os.path.join(col_dir, f"{candidate}.csv")
        pd.DataFrame({"row_index": df.index, col: df[col]}).to_csv(path, index=False, encoding="utf-8-sig")


# =========================
# Main
# =========================
def main():
    # Uso: python main.py [ruta_excel] [ruta_salida]
    excel_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "2025A.xlsx")
    outdir = sys.argv[2] if len(sys.argv) > 2 else "outputs"

    if not os.path.exists(excel_path):
        print(f"ERROR: No se encuentra el archivo Excel: {excel_path}")
        sys.exit(1)

    print(f"== Leyendo Excel: {excel_path}")
    frames = read_excel_all_sheets(excel_path, "all")

    print("== Unificando hojas y normalizando encabezados…")
    master = unify_sheets(frames)

    print("== Tipando columnas (SIMPLE: numérico vs string)…")
    inferred = build_simple_dtypes(master)
    master = apply_dtypes(master, inferred)

    print("== Exportando…")
    export_master_csv(master, outdir)
    export_columns_csvs(master, outdir)

    # Guarda el mapeo por referencia (útil para tu análisis posterior)
    with open(os.path.join(outdir, "inferred_dtypes.json"), "w", encoding="utf-8") as f:
        import json
        json.dump(inferred, f, indent=2, ensure_ascii=False)

    print("=== LISTO ===")
    print(f"Filas: {len(master)} | Columnas: {len(master.columns)}")
    print(f"Salida: {os.path.abspath(outdir)}")
    print("Generados:")
    print(" - master.csv")
    print(" - columns/*.csv (un archivo por columna)")
    print(" - inferred_dtypes.json (tipado aplicado)")

if __name__ == "__main__":
    main()
