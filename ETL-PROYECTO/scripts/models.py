from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base


class Pais(Base):
    __tablename__ = "paises"

    id = Column(Integer, primary_key=True, index=True)
    nombre_comun = Column(String, unique=True, nullable=False)
    nombre_oficial = Column(String)
    codigo_cca2 = Column(String)
    codigo_cca3 = Column(String)
    capital = Column(String)
    region = Column(String)
    subregion = Column(String)

    registros = relationship("DatosPais", back_populates="pais")


class DatosPais(Base):
    __tablename__ = "datos_paises"

    id = Column(Integer, primary_key=True, index=True)
    pais_id = Column(Integer, ForeignKey("paises.id"))

    poblacion = Column(Integer)
    area_km2 = Column(Float)
    latitud = Column(Float)
    longitud = Column(Float)
    continentes = Column(String)

    fecha_extraccion = Column(DateTime, default=datetime.utcnow)

    pais = relationship("Pais", back_populates="registros")


class MetricasETL(Base):
    __tablename__ = "metricas_etl"

    id = Column(Integer, primary_key=True, index=True)
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow)

    paises_procesados = Column(Integer)
    registros_insertados = Column(Integer)
    errores = Column(Integer)

    tiempo_ejecucion = Column(Float)
    estado = Column(String)