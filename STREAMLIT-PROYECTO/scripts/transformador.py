#!/usr/bin/env python3

import os
import pandas as pd


INPUT_PATH = "data/demo_paises_raw.csv"
OUTPUT_PATH = "data/demo_paises_transformado.csv"


def cargar_datos(ruta_entrada: str) -> pd.DataFrame:
    """Carga el archivo CSV de entrada."""
    if not os.path.exists(ruta_entrada):
        raise FileNotFoundError(f"❌ No se encontró el archivo: {ruta_entrada}")

    df = pd.read_csv(ruta_entrada)
    print(f"✅ Archivo cargado: {ruta_entrada}")
    print(f"📊 Registros originales: {len(df)}")
    return df


def limpiar_textos(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia espacios y normaliza columnas de texto."""
    columnas_texto = [
        "nombre_comun",
        "nombre_oficial",
        "codigo_cca2",
        "codigo_cca3",
        "capital",
        "region",
        "subregion",
        "continentes"
    ]

    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def convertir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas numéricas y fechas a tipos correctos."""
    columnas_enteras = ["poblacion"]
    columnas_float = ["area_km2", "latitud", "longitud"]

    for col in columnas_enteras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in columnas_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "fecha_extraccion" in df.columns:
        df["fecha_extraccion"] = pd.to_datetime(df["fecha_extraccion"], errors="coerce", utc=True)

    return df


def rellenar_faltantes(df: pd.DataFrame) -> pd.DataFrame:
    """Rellena valores faltantes con valores por defecto razonables."""
    valores_default = {
        "nombre_oficial": "No disponible",
        "capital": "N/A",
        "region": "Unknown",
        "subregion": "Unknown",
        "continentes": "Unknown"
    }

    for col, valor in valores_default.items():
        if col in df.columns:
            df[col] = df[col].fillna(valor)

    if "codigo_cca2" in df.columns:
        df["codigo_cca2"] = df["codigo_cca2"].fillna("XX")

    if "codigo_cca3" in df.columns:
        df["codigo_cca3"] = df["codigo_cca3"].fillna("XXX")

    if "area_km2" in df.columns:
        df["area_km2"] = df["area_km2"].fillna(0.0)

    if "latitud" in df.columns:
        df["latitud"] = df["latitud"].fillna(0.0)

    if "longitud" in df.columns:
        df["longitud"] = df["longitud"].fillna(0.0)

    return df


def eliminar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados por país, conservando el registro más reciente."""
    if "fecha_extraccion" in df.columns:
        df = df.sort_values(by="fecha_extraccion", ascending=False)

    if "nombre_comun" in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=["nombre_comun"], keep="first")
        despues = len(df)
        print(f"🧹 Duplicados eliminados: {antes - despues}")

    return df


def validar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica validaciones básicas de consistencia."""
    if "poblacion" in df.columns:
        df = df[df["poblacion"] >= 0]

    if "area_km2" in df.columns:
        df = df[df["area_km2"] >= 0]

    if "latitud" in df.columns:
        df = df[(df["latitud"] >= -90) & (df["latitud"] <= 90)]

    if "longitud" in df.columns:
        df = df[(df["longitud"] >= -180) & (df["longitud"] <= 180)]

    return df


def guardar_datos(df: pd.DataFrame, ruta_salida: str) -> None:
    """Guarda el DataFrame transformado en CSV."""
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    df.to_csv(ruta_salida, index=False, encoding="utf-8")
    print(f"✅ Archivo transformado guardado en: {ruta_salida}")
    print(f"📦 Registros finales: {len(df)}")


def transformar() -> None:
    """Pipeline completo de transformación."""
    df = cargar_datos(INPUT_PATH)
    df = limpiar_textos(df)
    df = convertir_tipos(df)
    df = rellenar_faltantes(df)
    df = eliminar_duplicados(df)
    df = validar_datos(df)
    guardar_datos(df, OUTPUT_PATH)

    print("\n📋 Primeras filas del dataset transformado:")
    print(df.head())


if __name__ == "__main__":
    transformar()