import os
import subprocess
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="ETL Países del Mundo",
    page_icon="🌍",
    layout="wide"
)

CSV_PATH = "data/paises.csv"


@st.cache_data
def cargar_datos():
    if os.path.exists(CSV_PATH):
        return pd.read_csv(CSV_PATH)
    return None


def ejecutar_etl():
    try:
        resultado = subprocess.run(
            ["python", "scripts/extractor_paises.py"],
            capture_output=True,
            text=True
        )
        return resultado
    except Exception as e:
        return str(e)


st.title("🌍 Dashboard ETL - Países del Mundo")
st.markdown("Aplicación en Streamlit para visualizar información de países obtenida desde RestCountries.")

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("🔄 Ejecutar ETL y actualizar datos"):
        with st.spinner("Ejecutando extracción..."):
            resultado = ejecutar_etl()
            if hasattr(resultado, "returncode"):
                if resultado.returncode == 0:
                    st.success("ETL ejecutado correctamente.")
                    st.text(resultado.stdout if resultado.stdout else "Proceso completado.")
                else:
                    st.error("Ocurrió un error al ejecutar el ETL.")
                    st.text(resultado.stderr)
            else:
                st.error(f"Error: {resultado}")

with col2:
    st.info("HOLA PROFE, TEQUEREMOS MUCHO")

df = cargar_datos()

if df is None or df.empty:
    st.warning("No se encontró `data/paises.csv`. Ejecuta primero el ETL.")
    st.stop()

# Limpieza básica
df["poblacion"] = pd.to_numeric(df["poblacion"], errors="coerce")
df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")

# Sidebar
st.sidebar.header("Filtros")

regiones = sorted(df["region"].dropna().unique().tolist())
region_seleccionada = st.sidebar.selectbox("Selecciona una región", ["Todas"] + regiones)

busqueda = st.sidebar.text_input("Buscar país por nombre")

df_filtrado = df.copy()

if region_seleccionada != "Todas":
    df_filtrado = df_filtrado[df_filtrado["region"] == region_seleccionada]

if busqueda:
    df_filtrado = df_filtrado[
        df_filtrado["nombre_comun"].str.contains(busqueda, case=False, na=False)
    ]

# Métricas
total_paises = len(df_filtrado)
poblacion_total = df_filtrado["poblacion"].sum(skipna=True)
area_total = df_filtrado["area_km2"].sum(skipna=True)

m1, m2, m3 = st.columns(3)
m1.metric("Total de países", f"{total_paises}")
m2.metric("Población total", f"{int(poblacion_total):,}".replace(",", "."))
m3.metric("Área total (km²)", f"{int(area_total):,}".replace(",", "."))

st.subheader("📋 Tabla de países")
st.dataframe(df_filtrado, use_container_width=True)

# Top 10 por población
st.subheader("📊 Top 10 países por población")

top_poblacion = df_filtrado.sort_values(by="poblacion", ascending=False).head(10)

if not top_poblacion.empty:
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(top_poblacion["nombre_comun"], top_poblacion["poblacion"])
    ax1.set_title("Top 10 países por población")
    ax1.set_xlabel("País")
    ax1.set_ylabel("Población")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig1)
else:
    st.info("No hay datos para mostrar en la gráfica de población.")

# Top 10 por área
st.subheader("🗺️ Top 10 países por área")

top_area = df_filtrado.sort_values(by="area_km2", ascending=False).head(10)

if not top_area.empty:
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.bar(top_area["nombre_comun"], top_area["area_km2"])
    ax2.set_title("Top 10 países por área")
    ax2.set_xlabel("País")
    ax2.set_ylabel("Área (km²)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig2)
else:
    st.info("No hay datos para mostrar en la gráfica de área.")

# Distribución por región
st.subheader("🌐 Cantidad de países por región")

conteo_regiones = df_filtrado["region"].value_counts()

if not conteo_regiones.empty:
    fig3, ax3 = plt.subplots(figsize=(8, 5))
    ax3.bar(conteo_regiones.index, conteo_regiones.values)
    ax3.set_title("Cantidad de países por región")
    ax3.set_xlabel("Región")
    ax3.set_ylabel("Cantidad")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig3)
else:
    st.info("No hay datos para mostrar por región.")