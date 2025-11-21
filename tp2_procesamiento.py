"""
TP2 - Procesamiento de datos (Carbon Intensity API)

En este TP2 trabajamos sobre los datos ya extraídos y almacenados en Delta
por el TP1 (capa bronze) y los transformamos para obtener una capa silver.

Tablas de origen (bronze):
- datalake/bronze/api_carbon_intensity/intensity
- datalake/bronze/api_carbon_intensity/factors

Objetivos:
- Limpiar y enriquecer la tabla de intensidad.
- Generar una tabla detallada (intervalos de 30 min).
- Generar una tabla agregada diaria.
- Procesar la tabla de factores para análisis exploratorio.

Transformaciones principales:
1) Conversión de tipos (fechas y numéricos).
2) Eliminación de duplicados.
3) Columnas de fecha (año, mes, día, hora, día de la semana).
4) Métrica unificada "intensity_value" (actual/forecast).
5) Categoría de nivel de intensidad (low/moderate/high/very high).
6) Agregaciones diarias (promedio, máximo, cantidad de intervalos).
7) Clasificación de factores (low/medium/high).
"""

import os
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv


# ==========================
# CONFIGURACIÓN GENERAL
# ==========================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATA_LAKE_PATH: Path = Path(os.getenv("DATA_LAKE_PATH", BASE_DIR / "datalake"))

# Rutas de BRONZE (origen)
BRONZE_ROOT: Path = DATA_LAKE_PATH / "bronze" / "api_carbon_intensity"
BRONZE_INTENSITY: Path = BRONZE_ROOT / "intensity"
BRONZE_FACTORS: Path = BRONZE_ROOT / "factors"

# Rutas de SILVER (destino)
SILVER_ROOT: Path = DATA_LAKE_PATH / "silver" / "api_carbon_intensity"
SILVER_INTENSITY: Path = SILVER_ROOT / "intensity"
SILVER_INTENSITY_DAILY: Path = SILVER_ROOT / "intensity_daily"
SILVER_FACTORS: Path = SILVER_ROOT / "factors"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)


# ==========================
# FUNCIONES AUXILIARES
# ==========================

def ensure_dir(path: Path) -> None:
    """Crea el directorio si no existe."""
    path.mkdir(parents=True, exist_ok=True)


def load_delta_to_df(path: Path) -> pd.DataFrame:
    """
    Carga una tabla Delta desde disco y la devuelve como DataFrame de pandas.
    """
    dt = DeltaTable(str(path))
    df = dt.to_pandas()
    return df


# ==========================
# TRANSFORMACIONES INTENSITY
# ==========================

def transform_intensity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones a la tabla de intensidad para generar la capa silver
    a nivel intervalo (30 minutos).
    """
    if df.empty:
        logging.warning("La tabla de intensidad en bronze está vacía.")
        return df

    # 1) Asegurar tipo datetime en 'from' y 'to'
    for col in ["from", "to"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # 2) Eliminamos posibles duplicados por clave natural ('from', 'to')
    df = df.drop_duplicates(subset=["from", "to"])

    # 3) Columnas de fecha para análisis y particionado
    df["date"] = df["from"].dt.date.astype(str)
    df["year"] = df["from"].dt.year
    df["month"] = df["from"].dt.month
    df["day"] = df["from"].dt.day
    df["hour"] = df["from"].dt.hour
    df["weekday"] = df["from"].dt.day_name()

    # 4) Unificamos una métrica de intensidad:
    #    usamos 'intensity.actual' y si está vacío tomamos 'intensity.forecast'
    df["intensity.actual"] = pd.to_numeric(
        df.get("intensity.actual"), errors="coerce"
    )
    df["intensity.forecast"] = pd.to_numeric(
        df.get("intensity.forecast"), errors="coerce"
    )

    df["intensity_value"] = df["intensity.actual"].fillna(
        df["intensity.forecast"]
    )

    # 5) Creamos una categoría de nivel de intensidad
    #    <=100    -> low
    #    100-200  -> moderate
    #    200-300  -> high
    #    >300     -> very high
    def classify_intensity(v):
        if pd.isna(v):
            return "unknown"
        if v <= 100:
            return "low"
        if v <= 200:
            return "moderate"
        if v <= 300:
            return "high"
        return "very high"

    df["intensity_level"] = df["intensity_value"].apply(classify_intensity)

    # 6) Timestamp de procesamiento de la capa silver
    df["processed_ts"] = datetime.now(timezone.utc).isoformat()

    return df


def build_intensity_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    A partir del detalle de intensidad, generamos una tabla diaria agregada.
    Calculamos el promedio, máximo y cantidad de intervalos por día.
    """
    if df.empty:
        return df

    # Nos aseguramos de que exista la columna 'date'
    if "date" not in df.columns:
        df["date"] = df["from"].dt.date.astype(str)

    # Agrupamos por día con agregaciones nombradas (más prolijo)
    agg = (
        df.groupby("date", as_index=False)
        .agg(
            intensity_mean=("intensity_value", "mean"),
            intensity_max=("intensity_value", "max"),
            interval_count=("intensity_value", "count"),
        )
    )

    # Año y mes como columnas separadas (útil para particionar)
    agg["year"] = pd.to_datetime(agg["date"]).dt.year
    agg["month"] = pd.to_datetime(agg["date"]).dt.month

    agg["processed_ts"] = datetime.now(timezone.utc).isoformat()

    return agg


# ==========================
# TRANSFORMACIONES FACTORS
# ==========================

def transform_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y enriquece la tabla de factores.
    """
    if df.empty:
        logging.warning("La tabla de factors en bronze está vacía.")
        return df

    # Normalizamos nombres de columnas
    df = df.rename(columns=lambda c: c.lower().strip())

    # Buscamos la columna de factor principal (gCO2/kWh)
    factor_cols = [c for c in df.columns if "gco2perkwh" in c]
    factor_col = factor_cols[0] if factor_cols else None

    if factor_col:
        df[factor_col] = pd.to_numeric(df[factor_col], errors="coerce")

        # Ordenamos por factor
        df = df.sort_values(by=factor_col)

        # Clasificación simple del factor
        def classify_factor(v):
            if pd.isna(v):
                return "unknown"
            if v <= 150:
                return "low"
            if v <= 400:
                return "medium"
            return "high"

        df["factor_level"] = df[factor_col].apply(classify_factor)

    df["processed_ts"] = datetime.now(timezone.utc).isoformat()

    return df


# ==========================
# PIPELINES PRINCIPALES
# ==========================

def process_intensity() -> None:
    """Pipeline completo para intensidad: bronze -> silver."""
    logging.info("Leyendo intensidad desde bronze...")
    df_bronze = load_delta_to_df(BRONZE_INTENSITY)
    logging.info(f"Registros bronze intensidad: {len(df_bronze)}")

    df_silver = transform_intensity(df_bronze)
    logging.info(f"Registros silver intensidad (detalle): {len(df_silver)}")

    # Guardamos detalle en silver, particionando por año/mes
    ensure_dir(SILVER_INTENSITY)
    write_deltalake(
        str(SILVER_INTENSITY),
        df_silver,
        mode="overwrite",
        partition_by=["year", "month"],
    )

    # Tabla diaria agregada
    df_daily = build_intensity_daily(df_silver)
    logging.info(f"Registros silver intensidad_daily: {len(df_daily)}")

    ensure_dir(SILVER_INTENSITY_DAILY)
    write_deltalake(
        str(SILVER_INTENSITY_DAILY),
        df_daily,
        mode="overwrite",
        partition_by=["year", "month"],
    )


def process_factors() -> None:
    """Pipeline completo para factors: bronze -> silver."""
    logging.info("Leyendo factors desde bronze...")
    df_bronze = load_delta_to_df(BRONZE_FACTORS)
    logging.info(f"Registros bronze factors: {len(df_bronze)}")

    df_silver = transform_factors(df_bronze)
    logging.info(f"Registros silver factors: {len(df_silver)}")

    ensure_dir(SILVER_FACTORS)
    write_deltalake(
        str(SILVER_FACTORS),
        df_silver,
        mode="overwrite",
    )


def main() -> None:
    logging.info("==== INICIO TP2 (Procesamiento) ====")
    process_intensity()
    process_factors()
    logging.info("==== FIN TP2 ====")


if __name__ == "__main__":
    main()
