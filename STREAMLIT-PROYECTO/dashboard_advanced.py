#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from sqlalchemy import func
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pais, DatosPais, MetricasETL

st.set_page_config(
    page_title="Dashboard Avanzado Países",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Dashboard Avanzado - Análisis de Países")
st.markdown("---")

db = SessionLocal()

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Vista General",
    "📈 Histórico",
    "🔍 Análisis",
    "📋 Métricas ETL"
])

with tab1:
    st.subheader("Datos Actuales")

    col1, col2, col3 = st.columns(3)

    with col1:
        paises_count = db.query(func.count(Pais.id)).scalar()
        st.metric("🌎 Países", paises_count)

    with col2:
        registros_count = db.query(func.count(DatosPais.id)).scalar()
        st.metric("📊 Registros Totales", registros_count)

    with col3:
        ultima_fecha = db.query(func.max(DatosPais.fecha_extraccion)).scalar()
        ultima_fecha_texto = ultima_fecha.strftime("%Y-%m-%d %H:%M") if ultima_fecha else "Sin datos"
        st.metric("⏰ Última Actualización", ultima_fecha_texto)

    st.markdown("---")

    registros_actuales = db.query(
        Pais.nombre_comun,
        Pais.region,
        DatosPais.poblacion,
        DatosPais.area_km2,
        DatosPais.continentes
    ).join(DatosPais).all()

    df_actual = pd.DataFrame(registros_actuales, columns=[
        "País", "Región", "Población", "Área km²", "Continente"
    ])

    if not df_actual.empty:
        col1, col2 = st.columns(2)

        with col1:
            top_poblacion = df_actual.sort_values("Población", ascending=False).head(10)
            fig = px.bar(
                top_poblacion,
                x="País",
                y="Población",
                title="Top 10 Países por Población",
                color="Población"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            conteo_region = df_actual["Región"].value_counts().reset_index()
            conteo_region.columns = ["Región", "Cantidad"]
            fig = px.pie(
                conteo_region,
                values="Cantidad",
                names="Región",
                title="Distribución por Región"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.dataframe(df_actual, use_container_width=True)
    else:
        st.warning("No hay datos disponibles en la base de datos.")

with tab2:
    st.subheader("Análisis Histórico")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio = st.date_input("Desde:", value=datetime.now() - timedelta(days=7))

    with col2:
        fecha_fin = st.date_input("Hasta:", value=datetime.now())

    registros_historicos = db.query(
        DatosPais,
        Pais.nombre_comun
    ).join(Pais).filter(
        DatosPais.fecha_extraccion >= fecha_inicio,
        DatosPais.fecha_extraccion <= fecha_fin
    ).all()

    if registros_historicos:
        data = []
        for registro, nombre_pais in registros_historicos:
            data.append({
                "Fecha": registro.fecha_extraccion,
                "País": nombre_pais,
                "Población": registro.poblacion,
                "Área km²": registro.area_km2,
                "Continente": registro.continentes
            })

        df_historico = pd.DataFrame(data)

        fig = px.line(
            df_historico,
            x="Fecha",
            y="Población",
            color="País",
            title="Población Registrada en el Tiempo",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.dataframe(df_historico, use_container_width=True)
    else:
        st.warning("No hay datos en ese rango de fechas.")

with tab3:
    st.subheader("Análisis Estadístico")

    paises = db.query(Pais).all()

    for pais in paises:
        with st.expander(f"📍 {pais.nombre_comun}"):
            registros = db.query(DatosPais).filter_by(pais_id=pais.id).all()

            if registros:
                poblaciones = [r.poblacion for r in registros if r.poblacion is not None]
                areas = [r.area_km2 for r in registros if r.area_km2 is not None]

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("👥 Población Prom.", f"{sum(poblaciones)/len(poblaciones):,.0f}" if poblaciones else "N/A")

                with col2:
                    st.metric("🗺️ Área Prom.", f"{sum(areas)/len(areas):,.2f} km²" if areas else "N/A")

                with col3:
                    st.metric("📊 Registros", len(registros))

                with col4:
                    st.metric("🌐 Región", pais.region if pais.region else "N/A")

with tab4:
    st.subheader("Métricas de Ejecución ETL")

    metricas = db.query(MetricasETL).order_by(
        MetricasETL.fecha_ejecucion.desc()
    ).limit(20).all()

    if metricas:
        data = []
        for m in metricas:
            data.append({
                "Fecha": m.fecha_ejecucion,
                "Estado": m.estado,
                "Países procesados": m.paises_procesados,
                "Registros insertados": m.registros_insertados,
                "Errores": m.errores,
                "Tiempo (s)": m.tiempo_ejecucion
            })

        df_metricas = pd.DataFrame(data)
        st.dataframe(df_metricas, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                df_metricas,
                x="Fecha",
                y="Registros insertados",
                title="Registros Insertados por Ejecución",
                color="Estado"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                df_metricas,
                x="Fecha",
                y="Tiempo (s)",
                size="Registros insertados",
                title="Duración de Ejecuciones",
                color="Estado"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay métricas registradas aún.")

db.close()