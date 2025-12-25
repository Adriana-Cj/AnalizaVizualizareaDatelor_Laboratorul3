#!/usr/bin/env python3
import cdsapi
import os
import time
import zipfile
import pandas as pd
from pathlib import Path

# Configurare API
url = "https://ads.atmosphere.copernicus.eu/api"
key = "e4ae2d8b-106c-4fa3-87cb-51082d64ae9e"

home_dir = os.path.expanduser("~")
config_file = os.path.join(home_dir, ".cdsapirc")

# Creare fișier de configurare
with open(config_file, 'w') as f:
    f.write(f"url: {url}\n")
    f.write(f"key: {key}\n")

client = cdsapi.Client()

dataset = "cams-europe-air-quality-forecasts-optimised-at-observation-sites"

# Parametri pentru întregul an 2024
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
days = [f"{i:02d}" for i in range(1, 32)]

output_file = "air_quality_poland_2024.csv"
temp_zip = "temp_download.zip"

print("=" * 70)
print("DESCĂRCARE DATE 2024 - POLONIA")
print("=" * 70)


def extract_csv_from_zip(zip_path, output_csv):
    """Extrage CSV din ZIP și returnează DataFrame"""
    print(f"  📦 Extragere CSV din {zip_path}...")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

            if not csv_files:
                print(f"  ✗ Nu s-au găsit fișiere CSV în arhivă!")
                return None

            print(f"  ✓ Găsite {len(csv_files)} fișiere CSV în arhivă")

            # Extrage și citește fiecare CSV
            dfs = []
            for csv_file in csv_files:
                print(f"    → Extragere: {csv_file}")
                with zip_ref.open(csv_file) as f:
                    df = pd.read_csv(f)
                    dfs.append(df)
                    print(f"      ✓ {len(df)} rânduri")

            # Combină toate DataFrame-urile
            if len(dfs) > 1:
                combined_df = pd.concat(dfs, ignore_index=True)
                print(f"  ✓ Combinat: {len(combined_df)} rânduri totale")
            else:
                combined_df = dfs[0]

            return combined_df

    except zipfile.BadZipFile:
        print(f"  ✗ Fișierul nu este un ZIP valid!")
        return None
    except Exception as e:
        print(f"  ✗ Eroare la extragere: {str(e)}")
        return None


# Verificare dacă fișierul final există deja
if os.path.exists(output_file):
    print(f"\n✓ {output_file} există deja!")
    size = os.path.getsize(output_file) / (1024 * 1024)
    df = pd.read_csv(output_file)
    print(f"  Dimensiune: {size:.2f} MB")
    print(f"  Rânduri: {len(df)}")
else:
    # Request pentru întregul an 2024, doar Polonia
    request = {
        "variable": ["nitrogen_dioxide", "particulate_matter_2.5um"],
        "country": ["poland"],
        "type": ["raw"],
        "leadtime_hour": ["0-23"],
        "year": ["2024"],
        "month": months,
        "day": days
    }

    print(f"\n⏳ Descărcare date Polonia pentru întregul an 2024...")
    print(f"   Variabile: NO2, PM2.5")
    print(f"   Perioada: 01.01.2024 - 31.12.2024")

    try:
        result = client.retrieve(dataset, request)
        result.download(temp_zip)

        if os.path.exists(temp_zip):
            size = os.path.getsize(temp_zip) / (1024 * 1024)
            print(f"\n✓ Descărcare completă: {size:.2f} MB")

            # Extrage CSV din ZIP
            df = extract_csv_from_zip(temp_zip, output_file)

            if df is not None:
                # Salvează DataFrame în CSV final
                df.to_csv(output_file, index=False)
                final_size = os.path.getsize(output_file) / (1024 * 1024)
                print(f"\n✓ Fișier final creat!")
                print(f"  Fișier: {output_file}")
                print(f"  Dimensiune: {final_size:.2f} MB")
                print(f"  Rânduri: {len(df)}")
                print(f"  Coloane: {list(df.columns)}")

                # Șterge ZIP temporar
                os.remove(temp_zip)
                print(f"  ✓ Șters fișier temporar")
            else:
                print("\n✗ Nu s-a putut extrage CSV-ul")
        else:
            print("\n✗ Eroare: Fișierul nu a fost descărcat")

    except Exception as e:
        error_msg = str(e)
        print(f"\n✗ Eroare la descărcare: {error_msg}")

        # Dacă requestul este prea mare, încearcă lună cu lună
        if "403" in error_msg or "too large" in error_msg or "cost limit" in error_msg:
            print("\n⚠️  Request-ul este prea mare. Descărc lună cu lună...")

            all_dfs = []

            for month in months:
                monthly_zip = f"temp_poland_2024_{month}.zip"

                request_monthly = {
                    "variable": ["nitrogen_dioxide", "particulate_matter_2.5um"],
                    "country": ["poland"],
                    "type": ["raw"],
                    "leadtime_hour": ["0-23"],
                    "year": ["2024"],
                    "month": [month],
                    "day": days
                }

                print(f"\n⏳ Descărcare luna {month}/2024...")

                try:
                    result = client.retrieve(dataset, request_monthly)
                    result.download(monthly_zip)

                    if os.path.exists(monthly_zip):
                        size = os.path.getsize(monthly_zip) / (1024 * 1024)
                        print(f"  ✓ Descărcat: {size:.2f} MB")

                        # Extrage CSV din ZIP
                        df = extract_csv_from_zip(monthly_zip, None)

                        if df is not None:
                            all_dfs.append(df)
                            print(f"  ✓ Procesat: {len(df)} rânduri")

                        # Șterge ZIP temporar
                        os.remove(monthly_zip)

                    time.sleep(2)

                except Exception as e2:
                    print(f"  ✗ Eroare luna {month}: {str(e2)}")
                    continue

            # Combinare toate lunile
            if all_dfs:
                print(f"\n⏳ Combinare {len(all_dfs)} luni...")

                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df.to_csv(output_file, index=False)

                final_size = os.path.getsize(output_file) / (1024 * 1024)
                print(f"\n✓ Fișier final creat!")
                print(f"  Fișier: {output_file}")
                print(f"  Dimensiune: {final_size:.2f} MB")
                print(f"  Rânduri: {len(combined_df)}")
                print(f"  Coloane: {list(combined_df.columns)}")

print("\n" + "=" * 70)
print("PROCES COMPLET!")
print("=" * 70)