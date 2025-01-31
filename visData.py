import matplotlib.pyplot as plt

# Convertir le DataFrame en Pandas
df_pandas = df.toPandas()

# Tracer un graphique
plt.bar(df_pandas["type_transaction"], df_pandas["montant_eur"])
plt.xlabel("Type de transaction")
plt.ylabel("Montant en EUR")
plt.title("Montant des transactions par type")
plt.show()