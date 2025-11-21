# TP – Pipeline de Datos con Carbon Intensity API

### Extracción · Procesamiento · Análisis · Delta Lake · Python

Este proyecto implementa un **pipeline de ingeniería de datos completo** utilizando la  
API pública **Carbon Intensity (UK)** para extraer, almacenar, procesar y visualizar información sobre la intensidad de carbono en intervalos de 30 minutos.

El trabajo cumple con los requisitos del Módulo 1 y 2 del TP:

- ✔ Extracción FULL e INCREMENTAL
- ✔ Almacenamiento en **Delta Lake** en una arquitectura tipo **data lake**
- ✔ Normalización, limpieza y enriquecimiento
- ✔ Capa **bronze** y **silver**
- ✔ Transformaciones obligatorias (y más)
- ✔ Vistas y gráficos opcionales (TP3)
- ✔ Código modular, comentado y buenas prácticas

---

## 🌍 API utilizada

**Carbon Intensity API – United Kingdom**  
https://api.carbonintensity.org.uk

### Endpoints:

| Endpoint                 | Tipo     | Uso                                               |
| ------------------------ | -------- | ------------------------------------------------- |
| `/intensity/{from}/{to}` | Temporal | **Extracción incremental** (intervalos de 30 min) |
| `/intensity/factors`     | Estático | **Extracción full** (factores por combustible)    |

---

# 🏗 Arquitectura del Proyecto

El pipeline está dividido en tres scripts:

### **1️⃣ TP1 – Extracción y almacenamiento (bronze)**

Archivo: `tp1_extraccion.py`

- Descarga los datos desde la API.
- Implementa:
  - **Extracción FULL** → factors
  - **Extracción INCREMENTAL** → intensity por ventanas
- Guarda todo en Delta Lake con estructura de data lake.

### **2️⃣ TP2 – Procesamiento y enriquecimiento (silver)**

Archivo: `tp2_procesamiento.py`

- Limpia, normaliza y transforma datos:
  - Conversión de tipos
  - Columnas derivadas (fecha, mes, hora, weekday)
  - Unificación de métrica `intensity_value`
  - Categorización de intensidad (`low`, `moderate`, `high`, `very high`)
  - Agregación diaria: promedio, máximo, cantidad de intervalos
  - Transformación de factors con clasificación por nivel
- Guarda las tablas procesadas en Delta Lake (silver)

### **3️⃣ TP3 – Vistas y gráficos (análisis exploratorio)**

Archivo: `tp3_vistas.py`

- Lee tablas silver.
- Muestra vistas (`head()`, `describe()`).
- Genera gráficos:
  - Intensidad media diaria
  - Distribución de niveles de intensidad
  - Factores por tipo de combustible (transpuestos)

---

# 📂 Estructura del Data Lake
