#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pais, DatosPais, MetricasETL

st.set_page_config(
    page_title="ETL Países del Mundo",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Dashboard ETL - Países del Mundo")
st.markdown("Aplicación en Streamlit para visualizar información de países almacenada en PostgreSQL.")

db = SessionLocal()

try:
    registros = db.query(Pais, DatosPais).join(DatosPais).all()

    data = []
    for pais, datos in registros:
        data.append({
            "nombre_comun": pais.nombre_comun,
            "nombre_oficial": pais.nombre_oficial,
            "capital": pais.capital,
            "region": pais.region,
            "subregion": pais.subregion,
            "poblacion": datos.poblacion,
            "area_km2": datos.area_km2,
            "latitud": datos.latitud,
            "longitud": datos.longitud,
            "continentes": datos.continentes,
            "fecha_extraccion": datos.fecha_extraccion
        })

    df = pd.DataFrame(data)

    if df.empty:
        st.warning("No hay datos en la base de datos. Ejecuta primero el ETL.")
        st.stop()

    df["poblacion"] = pd.to_numeric(df["poblacion"], errors="coerce")
    df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")

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

    total_paises = len(df_filtrado)
    poblacion_total = df_filtrado["poblacion"].sum(skipna=True)
    area_total = df_filtrado["area_km2"].sum(skipna=True)

    m1, m2, m3 = st.columns(3)
    m1.metric("Total de países", f"{total_paises}")
    m2.metric("Población total", f"{int(poblacion_total):,}".replace(",", "."))
    m3.metric("Área total (km²)", f"{int(area_total):,}".replace(",", "."))

    st.subheader("📋 Tabla de países")
    st.dataframe(df_filtrado, use_container_width=True)

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

    st.subheader("📈 Últimas ejecuciones del ETL")
    metricas = db.query(MetricasETL).order_by(MetricasETL.fecha_ejecucion.desc()).limit(5).all()

    if metricas:
        metricas_data = []
        for m in metricas:
            metricas_data.append({
                "Fecha": m.fecha_ejecucion,
                "Países procesados": m.paises_procesados,
                "Registros insertados": m.registros_insertados,
                "Errores": m.errores,
                "Tiempo ejecución": m.tiempo_ejecucion,
                "Estado": m.estado
            })

        df_metricas = pd.DataFrame(metricas_data)
        st.dataframe(df_metricas, use_container_width=True)

finally:
    db.close()