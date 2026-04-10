#!/usr/bin/env python3

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener la URL de conexión desde .env
DATABASE_URL = os.getenv("DATABASE_URL")

# Validar que exista
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL no está configurada en el archivo .env")

logger.info("Conectando a la base de datos atlas...")

# Crear engine de conexión
engine = create_engine(
    DATABASE_URL,
    echo=False
)

# Base para modelos ORM
Base = declarative_base()

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Metadata para inspeccionar la base de datos
metadata = MetaData()

try:
    metadata.reflect(bind=engine)
    logger.info("✅ Metadata cargada correctamente")
except Exception as e:
    logger.warning(f"⚠️ No se pudo reflejar metadata: {e}")


def get_db():
    """
    Obtiene una sesión de base de datos
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """
    Prueba la conexión a PostgreSQL
    """
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logger.info("✅ Conexión a PostgreSQL exitosa")
            return True

    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        return False


def create_all_tables():
    """
    Crea todas las tablas definidas en los modelos
    """
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas exitosamente")

    except Exception as e:
        logger.error(f"❌ Error creando tablas: {e}")