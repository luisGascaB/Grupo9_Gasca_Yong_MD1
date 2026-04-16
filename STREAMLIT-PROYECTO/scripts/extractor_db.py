#!/usr/bin/env python3

import os
import time
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

from scripts.database import SessionLocal
from scripts.models import Pais, DatosPais, MetricasETL

load_dotenv()


class PaisesETL:

    def __init__(self):
        self.input_path = "data/demo_paises_transformado.csv"
        self.db = SessionLocal()

        self.tiempo_inicio = time.time()
        self.paises_procesados = 0
        self.registros_insertados = 0
        self.errores = 0

    def extraer_datos(self):
        """Carga datos transformados desde CSV"""
        try:
            if not os.path.exists(self.input_path):
                raise FileNotFoundError(f"No se encontró el archivo: {self.input_path}")

            df = pd.read_csv(self.input_path)
            print(f"✅ Archivo cargado correctamente: {self.input_path}")
            print(f"📊 Registros encontrados: {len(df)}")

            return df.to_dict(orient="records")

        except Exception as e:
            print("Error cargando datos transformados:", e)
            self.errores += 1
            return []

    def guardar_datos(self, pais_data):
        """Guarda un país y sus datos asociados en PostgreSQL"""
        try:
            nombre_comun = pais_data.get("nombre_comun")

            if not nombre_comun:
                self.errores += 1
                return

            pais = self.db.query(Pais).filter_by(
                nombre_comun=nombre_comun
            ).first()

            capital = pais_data.get("capital", "N/A")
            region = pais_data.get("region", "Unknown")
            subregion = pais_data.get("subregion", "Unknown")
            continente = pais_data.get("continentes", "Unknown")

            if not pais:
                pais = Pais(
                    nombre_comun=nombre_comun,
                    nombre_oficial=pais_data.get("nombre_oficial"),
                    codigo_cca2=pais_data.get("codigo_cca2"),
                    codigo_cca3=pais_data.get("codigo_cca3"),
                    capital=capital,
                    region=region,
                    subregion=subregion
                )

                self.db.add(pais)
                self.db.flush()

            fecha_extraccion = pais_data.get("fecha_extraccion")
            if fecha_extraccion:
                try:
                    fecha_extraccion = pd.to_datetime(fecha_extraccion, utc=True).to_pydatetime()
                except Exception:
                    fecha_extraccion = datetime.now(timezone.utc)
            else:
                fecha_extraccion = datetime.now(timezone.utc)

            datos = DatosPais(
                pais_id=pais.id,
                poblacion=int(pais_data.get("poblacion", 0)) if pd.notna(pais_data.get("poblacion")) else 0,
                area_km2=float(pais_data.get("area_km2", 0.0)) if pd.notna(pais_data.get("area_km2")) else 0.0,
                latitud=float(pais_data.get("latitud", 0.0)) if pd.notna(pais_data.get("latitud")) else 0.0,
                longitud=float(pais_data.get("longitud", 0.0)) if pd.notna(pais_data.get("longitud")) else 0.0,
                continentes=continente,
                fecha_extraccion=fecha_extraccion
            )

            self.db.add(datos)
            self.db.commit()

            self.registros_insertados += 1

        except Exception as e:
            self.db.rollback()
            print("Error guardando:", e)
            self.errores += 1

    def guardar_metricas(self):
        """Guarda métricas de la ejecución ETL"""
        try:
            tiempo = time.time() - self.tiempo_inicio

            estado = "SUCCESS"
            if self.errores > 0:
                estado = "PARTIAL"

            metricas = MetricasETL(
                paises_procesados=self.paises_procesados,
                registros_insertados=self.registros_insertados,
                errores=self.errores,
                tiempo_ejecucion=tiempo,
                estado=estado
            )

            self.db.add(metricas)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            print("Error guardando métricas:", e)

    def ejecutar(self):
        """Ejecuta el proceso ETL completo"""
        datos = self.extraer_datos()

        for i, pais in enumerate(datos, start=1):
            self.paises_procesados += 1
            self.guardar_datos(pais)
            if i % 100 == 0:
                print(f"✅ Procesados: {i} registros")

        self.guardar_metricas()

        print("\nETL FINALIZADO")
        print("Paises procesados:", self.paises_procesados)
        print("Registros insertados:", self.registros_insertados)
        print("Errores:", self.errores)

        self.db.close()


if __name__ == "__main__":
    etl = PaisesETL()
    etl.ejecutar()