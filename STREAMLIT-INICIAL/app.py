import os
from datetime import datetime

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv

# Importa tu extractor desde scripts/
from scripts.extractor import WeatherstackExtractor

# Cargar variables de entorno
load_dotenv()

DATA_CSV_PATH = "data/clima.csv"
DATA_JSON_PATH = "data/clima_raw.json"

st.set_page_config(page_title="Dashboard Clima - Weatherstack", layout="wide")

st.title("🌦️ Dashboard de Clima por Ciudades (Weatherstack)")
st.caption("Extrae datos desde Weatherstack y visualiza temperatura, humedad, viento y sensación térmica.")

# Sidebar
st.sidebar.header("⚙️ Controles")

def run_extraction_and_save() -> pd.DataFrame:
    extractor = WeatherstackExtractor()
    datos = extractor.ejecutar_extraccion()
    df = pd.DataFrame(datos)

    # Asegurar carpeta data
    os.makedirs("data", exist_ok=True)

    # Guardar CSV (y JSON si quieres)
    df.to_csv(DATA_CSV_PATH, index=False)

    return df

def load_data_if_exists() -> pd.DataFrame | None:
    if os.path.exists(DATA_CSV_PATH):
        return pd.read_csv(DATA_CSV_PATH)
    return None

# Botón para extraer/actualizar
if st.sidebar.button("🚀 Extraer / Actualizar datos ahora"):
    with st.spinner("Extrayendo datos desde Weatherstack..."):
        try:
            df = run_extraction_and_save()
            st.sidebar.success("✅ Datos actualizados y guardados en data/clima.csv")
        except Exception as e:
            st.sidebar.error(f"❌ Error extrayendo datos: {e}")

# Cargar datos
df = load_data_if_exists()

if df is None or df.empty:
    st.warning("No hay datos todavía. Presiona **'Extraer / Actualizar datos ahora'** en el panel izquierdo.")
    st.stop()

# Normalizar tipos por si vienen como texto
for col in ["temperatura", "sensacion_termica", "humedad", "velocidad_viento"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Última actualización
last_update = None
if "fecha_extraccion" in df.columns and df["fecha_extraccion"].notna().any():
    last_update = df["fecha_extraccion"].dropna().max()

# Filtros
ciudades = sorted(df["ciudad"].dropna().unique().tolist())
selected_cities = st.sidebar.multiselect("🏙️ Filtrar ciudades", ciudades, default=ciudades)

df_f = df[df["ciudad"].isin(selected_cities)].copy()
if df_f.empty:
    st.info("No hay datos para las ciudades seleccionadas.")
    st.stop()

# Resumen arriba
colA, colB, colC, colD = st.columns(4)
colA.metric("Ciudades", len(df_f))
colB.metric("Temp. promedio (°C)", f"{df_f['temperatura'].mean():.1f}")
colC.metric("Humedad promedio (%)", f"{df_f['humedad'].mean():.1f}")
colD.metric("Viento promedio (km/h)", f"{df_f['velocidad_viento'].mean():.1f}")

if last_update:
    st.caption(f"🕒 Última extracción registrada: {last_update}")

st.divider()

# Tabla + descarga
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("📄 Datos")
    st.dataframe(df_f, use_container_width=True)

with c2:
    st.subheader("⬇️ Descargas")
    csv_bytes = df_f.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Descargar CSV filtrado",
        data=csv_bytes,
        file_name=f"clima_filtrado_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )

st.divider()

# Gráficas
st.subheader("📊 Visualizaciones")

g1, g2 = st.columns(2)

with g1:
    st.markdown("**Temperatura actual (°C)**")
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(df_f["ciudad"], df_f["temperatura"])
    ax.set_ylabel("°C")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    st.pyplot(fig, clear_figure=True)

with g2:
    st.markdown("**Humedad relativa (%)**")
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(df_f["ciudad"], df_f["humedad"])
    ax.set_ylabel("%")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    st.pyplot(fig, clear_figure=True)

g3, g4 = st.columns(2)

with g3:
    st.markdown("**Velocidad del viento (km/h)**")
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(df_f["ciudad"], df_f["velocidad_viento"], s=150)
    ax.set_ylabel("km/h")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(alpha=0.3)
    st.pyplot(fig, clear_figure=True)

with g4:
    st.markdown("**Temperatura vs Sensación térmica**")
    fig, ax = plt.subplots(figsize=(7, 4))
    x = np.arange(len(df_f))
    width = 0.35
    ax.bar(x - width/2, df_f["temperatura"], width, label="Temperatura")
    ax.bar(x + width/2, df_f["sensacion_termica"], width, label="Sensación")
    ax.set_ylabel("°C")
    ax.set_xticks(x)
    ax.set_xticklabels(df_f["ciudad"], rotation=45)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    st.pyplot(fig, clear_figure=True)