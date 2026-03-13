#!/usr/bin/env python3
import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Crear carpetas si no existen
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class RestCountriesExtractor:
    def __init__(self):
        self.base_url = os.getenv("RESTCOUNTRIES_BASE_URL", "https://restcountries.com/v3.1")

        # Máximo 10 fields para /all
        self.fields = [
            "name",
            "cca2",
            "cca3",
            "capital",
            "region",
            "subregion",
            "population",
            "area",
            "latlng",
            "continents"
        ]

    def extraer_paises(self):
        """Extrae información de todos los países"""
        try:
            url = f"{self.base_url}/all"
            params = {
                "fields": ",".join(self.fields)
            }

            logger.info("🌍 Iniciando extracción de países desde RestCountries...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                logger.error("❌ La respuesta no tiene el formato esperado")
                return []

            logger.info(f"✅ Se extrajeron {len(data)} registros de países")
            return data

        except requests.exceptions.Timeout:
            logger.error("❌ Timeout: la API tardó demasiado en responder")
            return []
        except requests.exceptions.ConnectionError:
            logger.error("❌ Error de conexión con la API")
            return []
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ Error HTTP: {str(e)}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error en la petición: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"❌ Error inesperado extrayendo datos: {str(e)}")
            return []

    def procesar_pais(self, pais):
        """Procesa un país y lo convierte a formato tabular"""
        try:
            nombre = pais.get("name", {})
            capital = pais.get("capital", [])
            latlng = pais.get("latlng", [])
            continents = pais.get("continents", [])

            return {
                "nombre_comun": nombre.get("common"),
                "nombre_oficial": nombre.get("official"),
                "codigo_cca2": pais.get("cca2"),
                "codigo_cca3": pais.get("cca3"),
                "capital": capital[0] if capital else None,
                "region": pais.get("region"),
                "subregion": pais.get("subregion"),
                "poblacion": pais.get("population"),
                "area_km2": pais.get("area"),
                "latitud": latlng[0] if len(latlng) > 0 else None,
                "longitud": latlng[1] if len(latlng) > 1 else None,
                "continentes": ", ".join(continents) if continents else None,
                "fecha_extraccion": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error procesando país: {str(e)}")
            return None

    def transformar_datos(self, data):
        """Transforma la lista cruda de países a una lista estructurada"""
        datos_procesados = []

        for pais in data:
            pais_procesado = self.procesar_pais(pais)
            if pais_procesado:
                datos_procesados.append(pais_procesado)

        logger.info(f"✅ Se procesaron {len(datos_procesados)} países correctamente")
        return datos_procesados

    def guardar_datos(self, datos):
        """Guarda los datos en JSON y CSV"""
        try:
            with open("data/paises_raw.json", "w", encoding="utf-8") as f:
                json.dump(datos, f, indent=2, ensure_ascii=False)
            logger.info("📁 Datos guardados en data/paises_raw.json")

            df = pd.DataFrame(datos)
            df.to_csv("data/paises.csv", index=False, encoding="utf-8-sig")
            logger.info("📁 Datos guardados en data/paises.csv")

            return df

        except Exception as e:
            logger.error(f"❌ Error guardando archivos: {str(e)}")
            return None

    def ejecutar_etl(self):
        """Ejecuta el flujo ETL completo"""
        datos_crudos = self.extraer_paises()

        if not datos_crudos:
            logger.warning("⚠️ No se extrajeron datos")
            return None

        datos_transformados = self.transformar_datos(datos_crudos)

        if not datos_transformados:
            logger.warning("⚠️ No se transformaron datos")
            return None

        df = self.guardar_datos(datos_transformados)
        return df


if __name__ == "__main__":
    try:
        extractor = RestCountriesExtractor()
        df = extractor.ejecutar_etl()

        if df is not None and not df.empty:
            print("\n" + "=" * 70)
            print("RESUMEN ETL - PAÍSES DEL MUNDO")
            print("=" * 70)
            print(df.head(10).to_string())
            print("=" * 70)
            print(f"Total países procesados: {len(df)}")
        else:
            print("No se generaron datos.")

    except Exception as e:
        logger.error(f"❌ Error general en el proceso ETL: {str(e)}")