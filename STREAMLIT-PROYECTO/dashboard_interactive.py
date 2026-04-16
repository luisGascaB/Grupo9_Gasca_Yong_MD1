#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from sqlalchemy import and_
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pais, DatosPais

st.set_page_config(
    page_title="Dashboard Interactivo",
    page_icon="🎛️",
    layout="wide"
)

st.markdown("""
    <style>
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - Control Total de Países")

db = SessionLocal()

st.sidebar.markdown("### 🔧 Controles")

regiones_disponibles = sorted([
    r[0] for r in db.query(Pais.region).distinct().all() if r[0]
])

regiones_seleccionadas = st.sidebar.multiselect(
    "🌍 Regiones a Mostrar",
    options=regiones_disponibles,
    default=regiones_disponibles[:2] if len(regiones_disponibles) >= 2 else regiones_disponibles
)

col1, col2 = st.sidebar.columns(2)
with col1:
    fecha_inicio = st.sidebar.date_input(
        "📅 Desde:",
        value=datetime.now() - timedelta(days=30)
    )
with col2:
    fecha_fin = st.sidebar.date_input(
        "📅 Hasta:",
        value=datetime.now()
    )

col1, col2 = st.sidebar.columns(2)
with col1:
    poblacion_min = st.sidebar.number_input("👥 Población mínima:", min_value=0, value=0, step=1000000)
with col2:
    area_min = st.sidebar.number_input("🗺️ Área mínima:", min_value=0.0, value=0.0, step=1000.0)

registros_filtrados = db.query(
    DatosPais,
    Pais.nombre_comun,
    Pais.region,
    Pais.subregion,
    Pais.capital
).join(Pais).filter(
    and_(
        Pais.region.in_(regiones_seleccionadas) if regiones_seleccionadas else True,
        DatosPais.fecha_extraccion >= fecha_inicio,
        DatosPais.fecha_extraccion <= fecha_fin,
        DatosPais.poblacion >= poblacion_min,
        DatosPais.area_km2 >= area_min
    )
).all()

data = []
for registro, nombre_pais, region, subregion, capital in registros_filtrados:
    data.append({
        'País': nombre_pais,
        'Capital': capital,
        'Región': region,
        'Subregión': subregion,
        'Población': registro.poblacion,
        'Área km²': registro.area_km2,
        'Latitud': registro.latitud,
        'Longitud': registro.longitud,
        'Continente': registro.continentes,
        'Fecha': registro.fecha_extraccion
    })

df = pd.DataFrame(data) if data else pd.DataFrame()

if not df.empty:
    st.markdown("### 📊 Indicadores Clave")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("🌍 Total Países", f"{df['País'].nunique()}")

    with col2:
        st.metric("👥 Población Máx", f"{df['Población'].max():,.0f}")

    with col3:
        st.metric("👥 Población Prom", f"{df['Población'].mean():,.0f}")

    with col4:
        st.metric("🗺️ Área Máx", f"{df['Área km²'].max():,.2f}")

    with col5:
        st.metric("🗺️ Área Prom", f"{df['Área km²'].mean():,.2f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Comparativa de Población")
        fig = px.box(
            df,
            x='Región',
            y='Población',
            color='Región',
            title='Distribución de Población por Región'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Promedio de Área por Región")
        area_region = df.groupby('Región')['Área km²'].mean().reset_index()
        fig = px.bar(
            area_region,
            x='Región',
            y='Área km²',
            color='Área km²'
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.markdown("#### 📈 Evolución Temporal")
    df['Fecha'] = pd.to_datetime(df['Fecha'])

    poblacion_tiempo = df.groupby(['Fecha', 'País'])['Población'].mean().reset_index()

    fig = px.line(
        poblacion_tiempo,
        x='Fecha',
        y='Población',
        color='País',
        title='Población Registrada en el Tiempo',
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.markdown("#### 📋 Datos Detallados")

    col1, col2 = st.columns(2)

    with col1:
        mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)

    with col2:
        columnas_mostrar = st.multiselect(
            "Columnas a mostrar:",
            df.columns.tolist(),
            default=['País', 'Capital', 'Región', 'Población', 'Área km²', 'Fecha']
        )

    if mostrar_todos:
        st.dataframe(df[columnas_mostrar], use_container_width=True, height=600)
    else:
        st.dataframe(df[columnas_mostrar].head(20), use_container_width=True)

    st.markdown("---")
    csv = df.to_csv(index=False)

    st.download_button(
        label="⬇️ Descargar datos como CSV",
        data=csv,
        file_name=f"paises_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

else:
    st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados")

db.close()