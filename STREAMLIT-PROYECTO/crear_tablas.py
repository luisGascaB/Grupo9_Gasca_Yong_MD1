from scripts.database import engine
from scripts.models import Base

# Crear todas las tablas definidas en los modelos
Base.metadata.create_all(bind=engine)

print("Tablas creadas correctamente")