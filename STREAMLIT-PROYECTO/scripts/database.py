#!/usr/bin/env python3
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener URL de la base de datos desde .env
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL no está configurada en el archivo .env")

# Crear engine de conexión
engine = create_engine(
    DATABASE_URL,
    echo=False  # poner True si quieres ver las consultas SQL
)

# Crear sesión de base de datos
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base para los modelos
Base = declarative_base()