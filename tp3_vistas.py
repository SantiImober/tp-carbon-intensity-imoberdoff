"""
TP3 - Vistas y gráficos a partir de la capa SILVER
(Carbon Intensity API)

Objetivo:
- Leer las tablas silver generadas en TP2.
- Crear algunas vistas simples de los datos (head, describe).
- Generar gráficos básicos para explorar la información.

Tablas usadas (silver):
- datalake/silver/api_carbon_intensity/intensity
- datalake/silver/api_carbon_intensity/intensity_daily
- datalake/silver/api_carbon_intensity/factors

Gráficos generados:
1) Serie temporal de intensidad media diaria.
2) Distribución de niveles de intensidad (low / moderate / high / very high).
3) Factores de intensidad por tipo de combustible (factors transpuesto).
"""

import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from deltalake import DeltaTable
from dotenv import load_dotenv


# ==========================
# CONFIGURACIÓN GENERAL
# ==========================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATA_LAKE_PATH: Path = Path(os.getenv("DATA_LAKE_PATH", BASE_DIR / "datalake"))

# Rutas a la capa SILVER
SILVER_ROOT: Path = DATA_LAKE_PATH / "silver" / "api_carbon_intensity"
SILVER_INTENSITY: Path = SILVER_ROOT / "intensity"
SILVER_INTENSITY_DAILY: Path = SILVER_ROOT / "intensity_daily"
SILVER_FACTORS: Path = SILVER_ROOT / "factors"

# Carpeta donde vamos a guardar los gráficos
FIGURES_DIR: Path = BASE_DIR / "figures"


# ==========================
# FUNCIONES AUXILIARES
# ==========================

def ensure_dir(path: Path) -> None:
    """Crea el directorio si no existe."""
    path.mkdir(parents=True, exist_ok=True)


def load_delta_to_df(path: Path) -> pd.DataFrame:
    """Lee una tabla Delta y la devuelve como DataFrame de pandas."""
    dt = DeltaTable(str(path))
    df = dt.to_pandas()
    return df


# ==========================
# FUNCIONES DE GRÁFICO
# ==========================

def plot_daily_intensity(df_daily: pd.DataFrame, output_dir: Path) -> None:
    """
    Gráfico 1:
    Serie temporal de la intensidad media diaria.
    """
    if df_daily.empty:
        print("⚠ No hay datos en intensity_daily.")
        return

    df_daily["date"] = pd.to_datetime(df_daily["date"])
    df_daily_sorted = df_daily.sort_values("date")

    plt.figure(figsize=(10, 5))
    plt.plot(df_daily_sorted["date"], df_daily_sorted["intensity_mean"], marker="o")
    plt.title("Intensidad media diaria (gCO2/kWh)")
    plt.xlabel("Fecha")
    plt.ylabel("Intensidad media (gCO2/kWh)")
    plt.grid(True)

    ensure_dir(output_dir)
    fname = output_dir / "daily_intensity_mean.png"
    plt.tight_layout()
    plt.savefig(fname)
    plt.close()

    print(f"✅ Gráfico guardado: {fname}")


def plot_intensity_level_distribution(df_detail: pd.DataFrame, output_dir: Path) -> None:
    """
    Gráfico 2:
    Distribución de niveles de intensidad (low / moderate / high / very high).
    """
    if df_detail.empty:
        print("⚠ No hay datos en intensity (detalle).")
        return

    counts = df_detail["intensity_level"].value_counts().sort_index()

    plt.figure(figsize=(8, 5))
    counts.plot(kind="bar")
    plt.title("Distribución de niveles de intensidad")
    plt.xlabel("Nivel de intensidad")
    plt.ylabel("Cantidad de intervalos")
    plt.xticks(rotation=0)

    ensure_dir(output_dir)
    fname = output_dir / "intensity_level_distribution.png"
    plt.tight_layout()    # <— CORRECTO
    plt.savefig(fname)
    plt.close()

    print(f"✅ Gráfico guardado: {fname}")


def plot_factors(df_factors: pd.DataFrame, output_dir: Path) -> None:
    """
    Gráfico 3:
    Factores de intensidad por tipo de combustible.

    El endpoint /intensity/factors devuelve una sola fila donde cada columna
    representa un combustible (biomass, coal, solar, wind, etc.) con su valor
    de gCO2/kWh. Por eso:

    - Eliminamos 'ingestion_ts'.
    - Transponemos la tabla para que cada combustible sea una fila.
    - Graficamos un barh comparando todos.
    """
    if df_factors.empty:
        print("⚠ No hay datos en factors.")
        return

    # Quitamos ingestion_ts porque no es un factor
    df = df_factors.drop(columns=["ingestion_ts"], errors="ignore")

    # Transponemos: columnas -> filas
    df_t = df.T.reset_index()
    df_t.columns = ["fuel", "gco2_value"]

    # Convertimos a numérico por si acaso
    df_t["gco2_value"] = pd.to_numeric(df_t["gco2_value"], errors="coerce")

    # Ordenamos de menor a mayor
    df_t = df_t.sort_values(by="gco2_value")

    plt.figure(figsize=(10, 8))
    plt.barh(df_t["fuel"], df_t["gco2_value"])
    plt.title("Factores de intensidad por combustible (gCO2/kWh)")
    plt.xlabel("gCO2/kWh")
    plt.ylabel("Combustible")
    plt.tight_layout()

    ensure_dir(output_dir)
    fname = output_dir / "factors_by_fuel.png"
    plt.savefig(fname)
    plt.close()

    print(f"✅ Gráfico FACTORS guardado: {fname}")


# ==========================
# MAIN / VISTAS
# ==========================

def main() -> None:
    print("==== TP3 - Vistas y gráficos (SILVER) ====")

    # 1) Leemos tablas silver
    print("\n📥 Leyendo tablas SILVER...")
    df_intensity = load_delta_to_df(SILVER_INTENSITY)
    df_intensity_daily = load_delta_to_df(SILVER_INTENSITY_DAILY)
    df_factors = load_delta_to_df(SILVER_FACTORS)

    print(f"Registros intensidad (detalle): {len(df_intensity)}")
    print(f"Registros intensidad_daily: {len(df_intensity_daily)}")
    print(f"Registros factors: {len(df_factors)}")

    # 2) Vistas rápidas
    print("\n👀 Vista rápida intensidad (detalle):")
    print(df_intensity.head())

    print("\n📊 Estadísticas básicas intensidad_value:")
    if "intensity_value" in df_intensity.columns:
        print(df_intensity["intensity_value"].describe())

    # 3) Gráficos
    print("\n📈 Generando gráficos...")

    ensure_dir(FIGURES_DIR)

    plot_daily_intensity(df_intensity_daily, FIGURES_DIR)
    plot_intensity_level_distribution(df_intensity, FIGURES_DIR)
    plot_factors(df_factors, FIGURES_DIR)

    print("\n✅ TP3 finalizado. Revisá los PNG en la carpeta 'figures'.")


if __name__ == "__main__":
    main()
