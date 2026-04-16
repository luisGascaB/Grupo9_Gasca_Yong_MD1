#!/usr/bin/env python3

import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd


REGIONES = [
    "Africa",
    "Americas",
    "Asia",
    "Europe",
    "Oceania"
]

SUBREGIONES = {
    "Africa": ["Northern Africa", "Western Africa", "Eastern Africa", "Middle Africa", "Southern Africa"],
    "Americas": ["North America", "Central America", "South America", "Caribbean"],
    "Asia": ["Western Asia", "Central Asia", "Eastern Asia", "Southern Asia", "Southeast Asia"],
    "Europe": ["Northern Europe", "Southern Europe", "Eastern Europe", "Western Europe"],
    "Oceania": ["Australia and New Zealand", "Melanesia", "Micronesia", "Polynesia"]
}

CONTINENTE_POR_REGION = {
    "Africa": "Africa",
    "Americas": "America",
    "Asia": "Asia",
    "Europe": "Europe",
    "Oceania": "Oceania"
}

PREFIJOS = [
    "Nueva", "Gran", "San", "Santa", "Puerto", "Villa", "Costa", "Monte",
    "Lago", "Valle", "Rio", "Isla", "Alto", "Bajo", "Sur", "Norte"
]

BASE_NOMBRES = [
    "Aurora", "Esperanza", "Libertad", "Andina", "Pacifica", "Centralia",
    "Monteluz", "Verdevia", "Solaria", "Terranova", "Marfilia", "Altamira",
    "Novaterra", "Brisalia", "Cordelia", "Florencia", "Rivania", "Lunaria",
    "Estrella", "Claridad", "Bosqueland", "Nubaria", "Cedral", "Venturia"
]

CAPITALES = [
    "Capital Nova", "Puerto Central", "Villa Norte", "Ciudad Esperanza",
    "San Aurelio", "Santa Marina", "Nueva Capital", "Monte Claro",
    "Puerto Azul", "Altavista", "Río Blanco", "Ciudad del Sol"
]


def generar_nombre_pais(indice: int) -> str:
    prefijo = random.choice(PREFIJOS)
    base = random.choice(BASE_NOMBRES)
    return f"{prefijo} {base} {indice}"


def generar_nombre_oficial(nombre_comun: str) -> str:
    formas = [
        f"República de {nombre_comun}",
        f"Estado de {nombre_comun}",
        f"Federación de {nombre_comun}",
        f"Reino de {nombre_comun}"
    ]
    return random.choice(formas)


def generar_codigo_cca2() -> str:
    letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return "".join(random.choices(letras, k=2))


def generar_codigo_cca3() -> str:
    letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return "".join(random.choices(letras, k=3))


def generar_latitud_longitud(region: str) -> tuple[float, float]:
    rangos = {
        "Africa": (-35.0, 37.0, -17.0, 51.0),
        "Americas": (-55.0, 72.0, -170.0, -30.0),
        "Asia": (-10.0, 80.0, 25.0, 180.0),
        "Europe": (35.0, 71.0, -25.0, 60.0),
        "Oceania": (-50.0, 0.0, 110.0, 180.0),
    }

    lat_min, lat_max, lon_min, lon_max = rangos[region]
    lat = round(random.uniform(lat_min, lat_max), 4)
    lon = round(random.uniform(lon_min, lon_max), 4)
    return lat, lon


def generar_fecha_extraccion() -> str:
    ahora = datetime.now(timezone.utc)
    dias_atras = random.randint(0, 120)
    segundos_extra = random.randint(0, 86400)
    fecha = ahora - timedelta(days=dias_atras, seconds=segundos_extra)
    return fecha.isoformat()


def generar_registro(indice: int) -> dict:
    region = random.choice(REGIONES)
    subregion = random.choice(SUBREGIONES[region])
    continente = CONTINENTE_POR_REGION[region]

    nombre_comun = generar_nombre_pais(indice)
    nombre_oficial = generar_nombre_oficial(nombre_comun)

    latitud, longitud = generar_latitud_longitud(region)

    poblacion = random.randint(50_000, 250_000_000)
    area_km2 = round(random.uniform(150.0, 17_500_000.0), 2)

    return {
        "nombre_comun": nombre_comun,
        "nombre_oficial": nombre_oficial,
        "codigo_cca2": generar_codigo_cca2(),
        "codigo_cca3": generar_codigo_cca3(),
        "capital": random.choice(CAPITALES),
        "region": region,
        "subregion": subregion,
        "poblacion": poblacion,
        "area_km2": area_km2,
        "latitud": latitud,
        "longitud": longitud,
        "continentes": continente,
        "fecha_extraccion": generar_fecha_extraccion()
    }


def generar_dataset(total_registros: int = 1000) -> pd.DataFrame:
    data = [generar_registro(i + 1) for i in range(total_registros)]
    return pd.DataFrame(data)


def guardar_csv(df: pd.DataFrame, ruta_salida: str) -> None:
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    df.to_csv(ruta_salida, index=False, encoding="utf-8")
    print(f"✅ Archivo generado: {ruta_salida}")
    print(f"📊 Registros generados: {len(df)}")


if __name__ == "__main__":
    ruta = "data/demo_paises_raw.csv"
    df = generar_dataset(1000)
    guardar_csv(df, ruta)
    print("\nPrimeras filas:")
    print(df.head())