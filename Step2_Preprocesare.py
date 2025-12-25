#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime

# Configurare
input_file = "air_quality_poland_2024.csv"
output_file = "air_quality_poland_2024_processed.csv"

print("=" * 70)
print("PREPROCESARE DATE CALITATE AER - POLONIA")
print("=" * 70)

# MAPARE REGIUNI POLONEZE
# Bazat pe sistemul NUTS-2 al Uniunii Europene pentru Polonia
POLAND_REGIONS = {
    '02': 'Dolnośląskie',  # Lower Silesian
    '04': 'Kujawsko-Pomorskie',  # Kuyavian-Pomeranian
    '06': 'Lubelskie',  # Lublin
    '08': 'Lubuskie',  # Lubusz
    '10': 'Łódzkie',  # Łódź
    '12': 'Małopolskie',  # Lesser Poland
    '14': 'Mazowieckie',  # Masovian
    '16': 'Opolskie',  # Opole
    '18': 'Podkarpackie',  # Subcarpathian
    '20': 'Podlaskie',  # Podlaskie
    '22': 'Pomorskie',  # Pomeranian
    '24': 'Śląskie',  # Silesian
    '26': 'Świętokrzyskie',  # Świętokrzyskie
    '28': 'Warmińsko-Mazurskie',  # Warmian-Masurian
    '30': 'Wielkopolskie',  # Greater Poland
    '32': 'Zachodniopomorskie',  # West Pomeranian
}


# Pentru coduri care nu se potrivesc cu NUTS-2, mapare pe bază de pattern
def map_region_code_to_name(region_code_str):
    """
    Mapează codul numeric al regiunii la numele real al voievodatului polonez
    """
    # Convertim la string cu padding
    code_str = str(region_code_str).zfill(2)

    # Verificăm în dicționar
    if code_str in POLAND_REGIONS:
        return POLAND_REGIONS[code_str]

    # Pentru coduri de 3 cifre (stații specifice), extragem primele 2 cifre
    if len(str(region_code_str)) >= 2:
        first_two = str(region_code_str)[:2]
        if first_two in POLAND_REGIONS:
            return POLAND_REGIONS[first_two]

    # Mapare extinsă pentru coduri specifice ale stațiilor
    region_num = int(region_code_str) if str(region_code_str).isdigit() else 0

    if region_num < 100:
        # Coduri sub 100 - mapare directă
        code_key = f"{region_num:02d}"
        return POLAND_REGIONS.get(code_key, f"Region-{region_num}")
    else:
        # Coduri peste 100 - mapare pe bază de range
        if 200 <= region_num < 300:
            return 'Dolnośląskie'
        elif 300 <= region_num < 400:
            return 'Kujawsko-Pomorskie'
        elif 400 <= region_num < 500:
            return 'Lubelskie'
        elif 500 <= region_num < 600:
            return 'Lubuskie'
        elif 600 <= region_num < 700:
            return 'Łódzkie'
        elif 100 <= region_num < 200:
            return 'Małopolskie'
        elif 700 <= region_num < 800:
            return 'Mazowieckie'
        elif 800 <= region_num < 900:
            return 'Opolskie'
        elif 900 <= region_num < 1000:
            return 'Podkarpackie'
        elif 1000 <= region_num < 1100:
            return 'Podlaskie'
        elif 1100 <= region_num < 1200:
            return 'Pomorskie'
        elif 1200 <= region_num < 1300:
            return 'Śląskie'
        elif 1300 <= region_num < 1400:
            return 'Świętokrzyskie'
        elif 1400 <= region_num < 1500:
            return 'Warmińsko-Mazurskie'
        elif 1500 <= region_num < 1600:
            return 'Wielkopolskie'
        elif 1600 <= region_num < 1700:
            return 'Zachodniopomorskie'
        else:
            return f"Unknown-{region_num}"


# 1. CITIRE FIȘIER
print("\n📂 Citire fișier CSV...")
try:
    df = pd.read_csv(input_file, sep=';')
    print(f"✓ Citit: {len(df):,} rânduri, {len(df.columns)} coloane")
    print(f"  Coloane: {list(df.columns)}")
except Exception as e:
    print(f"✗ Eroare la citire: {str(e)}")
    exit(1)

# 2. VERIFICARE VALORI NULL
print("\n🔍 Verificare valori NULL...")
null_counts = df.isnull().sum()
total_nulls = null_counts.sum()

if total_nulls > 0:
    print(f"⚠️  Găsite {total_nulls} valori NULL:")
    for col, count in null_counts[null_counts > 0].items():
        print(f"  - {col}: {count} valori NULL ({count / len(df) * 100:.2f}%)")

    rows_before = len(df)
    df = df.dropna()
    rows_after = len(df)
    print(f"✓ Eliminate {rows_before - rows_after:,} rânduri cu valori NULL")
    print(f"  Rânduri rămase: {rows_after:,}")
else:
    print("✓ Nu există valori NULL")

# 3. VERIFICARE TIPURI DE DATE
print("\n🔍 Verificare tipuri de date...")
print("Tipuri curente:")
for col in df.columns:
    print(f"  - {col}: {df[col].dtype}")

# Verificare dacă conc_raw_micrograms_per_m3 este numeric
if df['conc_raw_micrograms_per_m3'].dtype not in ['float64', 'float32', 'int64', 'int32']:
    print("\n⚠️  Conversie concentrație la float...")
    df['conc_raw_micrograms_per_m3'] = pd.to_numeric(df['conc_raw_micrograms_per_m3'], errors='coerce')

    nulls_after = df['conc_raw_micrograms_per_m3'].isnull().sum()
    if nulls_after > 0:
        print(f"  ⚠️  Eliminate {nulls_after} rânduri cu valori non-numerice")
        df = df.dropna(subset=['conc_raw_micrograms_per_m3'])

print("✓ Verificare completă")

# 4. DECODIFICARE CODURI STAȚII
print("\n🗺️  Decodificare coduri stații...")


# Funcție pentru extragere cod regiune numeric
def extract_region_code(station_id):
    """Extrage codul numeric al regiunii din station_id"""
    if len(station_id) >= 6:
        region_part = station_id[2:6]
        # Extrage doar cifrele
        region_digits = ''.join(filter(str.isdigit, region_part))
        return int(region_digits) if region_digits else 0
    return 0


# Funcție pentru generare cod regiune formatat
def format_region_code(station_id):
    """Generează codul regiunii în format PL-XXX"""
    region_code = extract_region_code(station_id)
    return f"PL-{region_code:03d}"


# Aplicare funcții
df['country_name'] = 'Poland'  # Toate datele sunt din Polonia
df['region_code'] = df['station_id'].apply(extract_region_code)
df['region_name'] = df['region_code'].apply(map_region_code_to_name)
df['region_code_formatted'] = df['station_id'].apply(format_region_code)

print(f"✓ Procesate date pentru Polonia")
print(f"✓ Identificate {df['region_code'].nunique()} coduri unice de regiune")
print(f"✓ Identificate {df['region_name'].nunique()} voievodate unice")

# Afișare mapare regiuni
print("\n📍 Mapare regiuni identificate:")
region_mapping = df[['region_code', 'region_name']].drop_duplicates().sort_values('region_code')
for _, row in region_mapping.iterrows():
    count = len(df[df['region_code'] == row['region_code']])
    print(f"  Cod {row['region_code']:3d} → {row['region_name']:25s} ({count:,} măsurători)")

# 5. PROCESARE DATETIME
print("\n📅 Procesare date și timp...")

# Conversie datetime
df['datetime'] = pd.to_datetime(df['datetime'])

# Extragere componente
df['date'] = df['datetime'].dt.date
df['hour'] = df['datetime'].dt.hour
df['month'] = df['datetime'].dt.month
df['day_of_week'] = df['datetime'].dt.day_name()

print("✓ Extrase componente temporale:")
print(f"  - Date: {df['date'].min()} → {df['date'].max()}")
print(f"  - Ore: {df['hour'].min()} → {df['hour'].max()}")
print(f"  - Luni: {sorted(df['month'].unique())}")

# 6. REORGANIZARE COLOANE
print("\n📋 Reorganizare coloane...")

# Ordinea finală a coloanelor (folosim region_code_formatted în loc de region_name vechi)
final_columns = [
    'station_id',
    'country_name',
    'region_code',
    'region_name',
    'date',
    'hour',
    'month',
    'day_of_week',
    'species',
    'conc_raw_micrograms_per_m3'
]

df_final = df[final_columns].copy()

print(f"✓ Coloane finale: {list(df_final.columns)}")

# 7. STATISTICI FINALE
print("\n📊 Statistici finale...")
print(f"  Total rânduri: {len(df_final):,}")
print(f"  Stații unice: {df_final['station_id'].nunique()}")
print(f"  Regiuni: {df_final['region_name'].nunique()}")
print(f"  Specii: {', '.join(df_final['species'].unique())}")
print(f"  Perioada: {df_final['date'].min()} → {df_final['date'].max()}")

# Statistici pe specie
print(f"\n  Concentrații pe specie:")
for species in df_final['species'].unique():
    species_data = df_final[df_final['species'] == species]['conc_raw_micrograms_per_m3']
    print(f"    {species}:")
    print(f"      - Min: {species_data.min():.2f} μg/m³")
    print(f"      - Max: {species_data.max():.2f} μg/m³")
    print(f"      - Medie: {species_data.mean():.2f} μg/m³")
    print(f"      - Mediană: {species_data.median():.2f} μg/m³")

# 8. SALVARE FIȘIER
print(f"\n💾 Salvare fișier: {output_file}...")

try:
    df_final.to_csv(output_file, sep=';', index=False)
    import os

    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"✓ Salvat: {file_size:.2f} MB")

    # Afișare primele rânduri
    print("\n📋 Primele 10 rânduri din fișierul procesat:")
    print(df_final.head(10).to_string(index=False))

except Exception as e:
    print(f"✗ Eroare la salvare: {str(e)}")
    exit(1)

print("\n" + "=" * 70)
print("PREPROCESARE COMPLETĂ!")
print("=" * 70)
print(f"\n✓ Fișier de intrare: {input_file}")
print(f"✓ Fișier de ieșire: {output_file}")
print(f"✓ Procesate: {len(df_final):,} rânduri")
print(f"✓ Calitate date: 100% (fără valori NULL)")
print(f"✓ Regiuni mapate: {df_final['region_name'].nunique()} voievodate")