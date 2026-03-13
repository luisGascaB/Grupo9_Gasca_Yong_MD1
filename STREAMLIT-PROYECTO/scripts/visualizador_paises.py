#!/usr/bin/env python3
import os
import pandas as pd
import matplotlib.pyplot as plt

os.makedirs("data", exist_ok=True)

# Cargar datos
df = pd.read_csv("data/paises.csv")

# Tomar top 10 países por población
top_poblacion = df.sort_values(by="poblacion", ascending=False).head(10)

plt.figure(figsize=(14, 8))
plt.bar(top_poblacion["nombre_comun"], top_poblacion["poblacion"])
plt.title("Top 10 países por población")
plt.xlabel("País")
plt.ylabel("Población")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("data/top_10_poblacion.png", dpi=300)
plt.show()