"""
TP1 - Extracción y almacenamiento de datos usando la API Carbon Intensity (UK)

✔ API elegida:
    https://api.carbonintensity.org.uk

✔ Endpoints usados:
    1) /intensity/{from}/{to}
         - Devuelve datos de intensidad de carbono en bloques de 30 minutos.
         - Lo usamos como extracción INCREMENTAL (solo traemos lo nuevo).

    2) /intensity/factors
         - Devuelve factores de intensidad por tipo de combustible.
         - Lo usamos como extracción FULL (catalogo completo).

✔ Estrategia general:
    - Hacemos una extracción INCREMENTAL inteligente:
        cada vez traemos solo lo que falta según lo guardado en Delta Lake.
    - Evitamos duplicados usando un "UPSERT" simple
        (unimos datos nuevos con existentes y eliminamos duplicados).
    - Guardamos todo en Delta Lake con estructura de data lake limpia.

✔ Estructura del Data Lake (bronze):
    datalake/
      bronze/
        api_carbon_intensity/
          intensity/   -> incremental
          factors/     -> full

✔ Buenas prácticas aplicadas:
    - Configuración externa en .env (nada hardcodeado).
    - .gitignore para no subir .venv / datos a GitHub.
    - Código modular, comentado y ordenado.
    - Manejo de errores y reintentos en llamados a la API.
"""

# ==============================
# IMPORTS
# ==============================

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import requests
from deltalake import write_deltalake, DeltaTable
from dotenv import load_dotenv


# ==============================
# CONFIGURACIÓN GENERAL
# ==============================

# Carpeta donde está este archivo. Esto nos permite construir rutas limpias.
BASE_DIR = Path(__file__).resolve().parent

# Cargamos las variables escritas en el archivo .env
load_dotenv(BASE_DIR / ".env")

# URL base de la API (no hardcodeada)
BASE_URL: str = os.getenv("BASE_URL", "https://api.carbonintensity.org.uk")

# Ruta principal del data lake (también configurable vía .env)
DATA_LAKE_PATH: Path = Path(os.getenv("DATA_LAKE_PATH", BASE_DIR / "datalake"))

BRONZE_ROOT: Path = DATA_LAKE_PATH / "bronze" / "api_carbon_intensity"
INTENSITY_PATH: Path = BRONZE_ROOT / "intensity"   # incremental
FACTORS_PATH: Path = BRONZE_ROOT / "factors"       # full

# Configuracion logging para tener trazabilidad del proceso
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)


# ==============================
# FUNCIONES DE UTILIDAD
# ==============================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def call_api(path: str, params: Optional[dict] = None, retries: int = 3) -> dict:
   
    url = f"{BASE_URL}{path}"

    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Llamando a la API: {url}")
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as err:
            logging.warning(f"Error al llamar la API (intento {attempt}): {err}")

            
            if attempt == retries:
                raise

            
            time.sleep(2)


# ==============================
# EXTRACCIÓN DESDE LA API
# ==============================

def get_intensity_range(from_iso: str, to_iso: str) -> pd.DataFrame:
    """
    Extrae intensidad de carbono entre dos fechas/hours ISO.
    Esto es lo que usamos como extracción INCREMENTAL.
    """
    path = f"/intensity/{from_iso}/{to_iso}"
    data = call_api(path)
    results = data.get("data", [])
    return pd.json_normalize(results)


def get_intensity_factors() -> pd.DataFrame:
    """
    Extrae el catálogo completo de factores.
    """
    data = call_api("/intensity/factors")
    results = data.get("data", [])
    return pd.json_normalize(results)


# ==============================
# LÓGICA DE INCREMENTAL
# ==============================

def is_delta_table(path: Path) -> bool:
    """Detecta si en esa carpeta ya existe una tabla Delta."""
    try:
        return DeltaTable.is_deltatable(str(path))
    except Exception:
        return False


def get_last_from_timestamp(delta_path: Path) -> Optional[datetime]:
    """
    Busca el último 'from' guardado en la tabla Delta de intensidad.
    Esto nos permite saber desde cuándo debemos continuar el incremental.
    """
    if not is_delta_table(delta_path):
        return None

    dt = DeltaTable(str(delta_path))
    df = dt.to_pandas()

    if "from" not in df.columns or df.empty:
        return None

    # Convertimos 'from' a datetime para poder sacar el máximo
    df["from_dt"] = pd.to_datetime(df["from"], utc=True, errors="coerce")
    return df["from_dt"].max()


def compute_incremental_window(delta_path: Path) -> Optional[Tuple[str, str]]:
    """
    Calcula la ventana del incremental:
    - Si nunca corrimos → trae últimos 7 días.
    - Si ya corrimos → trae desde último 'from' + 30 min hasta ahora.
    """
    now = datetime.now(timezone.utc)
    last_from = get_last_from_timestamp(delta_path)

    if last_from is None:
        logging.info("Primera corrida: cargamos últimos 7 días.")
        from_dt = now - timedelta(days=7)
    else:
        logging.info(f"Último intervalo encontrado: {last_from}")
        from_dt = last_from + timedelta(minutes=30)

    if from_dt >= now:
        logging.info("No hay nuevos intervalos para cargar.")
        return None

    return from_dt.isoformat(), now.isoformat()


# ==============================
# NORMALIZACIÓN DE DATAFRAMES
# ==============================

def normalize_intensity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y enriquece los datos:
    - Convierte fechas.
    - Asegura tipos numéricos.
    - Crea particiones por día.
    """
    if df.empty:
        return df

    # Convertimos tiempo
    for col in ["from", "to"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)

    # Columnas numéricas
    for c in df.columns:
        if c.startswith("intensity."):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Partición por fecha (clave para Delta Lake)
    df["date_part"] = df["from"].dt.date.astype(str)

    # Timestamp de ingesta
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    return df


def normalize_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza y deja prolija la tabla de factores.
    """
    if df.empty:
        return df

    df = df.rename(columns=lambda c: c.lower().strip())
    num_cols = [c for c in df.columns if "gco2" in c]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
    return df


# ==============================
# UPSERT EN DELTA LAKE
# ==============================

def upsert_delta(df_new: pd.DataFrame, delta_path: Path, key_cols: List[str], partition_cols: List[str]):
    """
    Realiza un UPSERT manual:
    - Si la tabla no existe: la crea.
    - Si existe: lee la existente, une con la nueva y elimina duplicados.
    """
    ensure_dir(delta_path)

    if not is_delta_table(delta_path):
        write_deltalake(str(delta_path), df_new, mode="overwrite", partition_by=partition_cols)
        return

    dt = DeltaTable(str(delta_path))
    df_old = dt.to_pandas()

    # Combino y elimino duplicados por claves naturales ("from","to")
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=key_cols)

    write_deltalake(str(delta_path), df_all, mode="overwrite", partition_by=partition_cols)


# ==============================
# FUNCIONES PRINCIPALES DEL TP
# ==============================

def run_incremental_intensity():
    """Orquesta la carga incremental del endpoint /intensity/."""
    window = compute_incremental_window(INTENSITY_PATH)
    if window is None:
        return

    from_iso, to_iso = window
    logging.info(f"Extrayendo intensidad desde {from_iso} hasta {to_iso}")

    df_raw = get_intensity_range(from_iso, to_iso)
    df_norm = normalize_intensity(df_raw)

    upsert_delta(
        df_new=df_norm,
        delta_path=INTENSITY_PATH,
        key_cols=["from", "to"],
        partition_cols=["date_part"],
    )


def run_full_factors():
    """Carga completa del catálogo /intensity/factors."""
    logging.info("Extrayendo factores de intensidad (FULL).")

    df_raw = get_intensity_factors()
    df_norm = normalize_factors(df_raw)

    ensure_dir(FACTORS_PATH)
    write_deltalake(str(FACTORS_PATH), df_norm, mode="overwrite")


# ==============================
# MAIN
# ==============================

def main():
    logging.info("==== INICIO TP1 ====")
    run_incremental_intensity()
    run_full_factors()
    logging.info("==== FIN TP1 ====")


if __name__ == "__main__":
    main()