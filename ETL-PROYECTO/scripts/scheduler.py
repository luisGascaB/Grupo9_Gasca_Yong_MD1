#!/usr/bin/env python3
import time
import schedule
import logging
from extractor_paises import RestCountriesExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def ejecutar_etl():
    logger.info("⏰ Ejecutando ETL programado de países...")
    extractor = RestCountriesExtractor()
    extractor.ejecutar_etl()


# Ejecutar cada 24 horas
schedule.every(24).hours.do(ejecutar_etl)

if __name__ == "__main__":
    logger.info("🚀 Scheduler iniciado. Esperando próximas ejecuciones...")

    # Ejecutar una vez al inicio
    ejecutar_etl()

    while True:
        schedule.run_pending()
        time.sleep(60)