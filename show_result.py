import pandas as pd

# Load hasil target (file CSV yang telah disimpan sebelumnya)
df = pd.read_csv("earthquake_clean.csv")

print("📊 5 Data Teratas:")
print(df.head())

print("\n🔍 Gempa dengan Magnitudo > 5:")
print(df[df["mag"] > 5])

print("\n📍 Jumlah Gempa berdasarkan Lokasi:")
print(df["place"].value_counts().head(10))

print("\n🔥 Gempa Terbesar:")
print(df[df["mag"] == df["mag"].max()])
