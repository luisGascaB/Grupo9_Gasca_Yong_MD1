#!/usr/bin/env python3

import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pais, DatosPais, MetricasETL
from sqlalchemy import func
import pandas as pd

db = SessionLocal()


def poblacion_promedio_por_region():
    """Población promedio por región"""
    registros = db.query(
        Pais.region,
        func.avg(DatosPais.poblacion).label("poblacion_promedio")
    ).join(DatosPais).group_by(Pais.region).all()

    df = pd.DataFrame(registros, columns=["Región", "Población Promedio"])
    print("\n📊 POBLACIÓN PROMEDIO POR REGIÓN:")
    print(df.to_string(index=False))


def pais_mayor_poblacion():
    """Identifica el país con mayor población"""
    registro = db.query(
        Pais.nombre_comun,
        DatosPais.poblacion
    ).join(DatosPais).order_by(
        DatosPais.poblacion.desc()
    ).first()

    if registro:
        print(f"\n👥 PAÍS CON MAYOR POBLACIÓN: {registro.nombre_comun} con {registro.poblacion} habitantes")


def pais_mayor_area():
    """Identifica el país con mayor área"""
    registro = db.query(
        Pais.nombre_comun,
        DatosPais.area_km2
    ).join(DatosPais).order_by(
        DatosPais.area_km2.desc()
    ).first()

    if registro:
        print(f"\n🗺️ PAÍS CON MAYOR ÁREA: {registro.nombre_comun} con {registro.area_km2} km²")


def cantidad_paises_por_continente():
    """Cuenta cuántos países hay por continente"""
    registros = db.query(
        DatosPais.continentes,
        func.count(Pais.id).label("cantidad_paises")
    ).join(Pais).group_by(DatosPais.continentes).all()

    df = pd.DataFrame(registros, columns=["Continente", "Cantidad de Países"])
    print("\n🌍 CANTIDAD DE PAÍSES POR CONTINENTE:")
    print(df.to_string(index=False))


def metricas_etl():
    """Muestra métricas de ejecuciones"""
    metricas = db.query(MetricasETL).order_by(
        MetricasETL.fecha_ejecucion.desc()
    ).limit(5).all()

    print("\n📈 ÚLTIMAS 5 EJECUCIONES DEL ETL:")
    for m in metricas:
        print(f"  - {m.fecha_ejecucion}: {m.estado} ({m.registros_insertados} registros en {m.tiempo_ejecucion:.2f}s)")


if __name__ == "__main__":
    try:
        print("\n" + "=" * 50)
        print("ANÁLISIS DE DATOS - POSTGRESQL")
        print("=" * 50)

        poblacion_promedio_por_region()
        pais_mayor_poblacion()
        pais_mayor_area()
        cantidad_paises_por_continente()
        metricas_etl()

        print("\n" + "=" * 50 + "\n")

    finally:
        db.close()
        w