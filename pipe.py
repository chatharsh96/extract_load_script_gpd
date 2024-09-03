import requests
import tempfile
import zipfile
import os
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from functools import partial

def download_file(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def extract_gpkg(content):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
        temp_file.write(content)
        temp_file_path = temp_file.name

    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        data_dir = os.path.join(temp_dir, 'data')
        gpkg_file = next((os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.gpkg')), None)
        
        if not gpkg_file:
            raise FileNotFoundError("No .gpkg file found in the extracted data directory.")
        
        gdf = gpd.read_file(gpkg_file)

    os.unlink(temp_file_path)
    return gdf

def prepare_gdf(gdf):
    # Perform any necessary data cleaning or transformation
    gdf = gdf.to_crs(epsg=4326)  # Convert to WGS84
    return gdf

def ingest_to_postgis(gdf, table_name, db_url):
    engine = create_engine(db_url)
    gdf.to_postgis(table_name, engine, if_exists='replace')
    return f"Data ingested to {table_name}"

def geopackage_to_postgis_pipeline(url, table_name, db_url):
    return (
        pd.DataFrame({'url': [url]})
        .pipe(lambda df: df.assign(content=df['url'].apply(download_file)))
        .pipe(lambda df: df.assign(gdf=df['content'].apply(extract_gpkg)))
        .pipe(lambda df: df.assign(gdf=df['gdf'].apply(prepare_gdf)))
        .pipe(lambda df: df.assign(result=df['gdf'].apply(partial(ingest_to_postgis, table_name=table_name, db_url=db_url))))
        ['result'].iloc[0]
    )

if __name__ == "__main__":
    url = "https://api.os.uk/downloads/v1/products/OpenGreenspace/downloads?area=GB&format=GeoPackage&redirect"
    table_name = "my_tablee"
    db_url = "postgresql://username:password@localhost:5432/postgis_34_sample"
    
    result = geopackage_to_postgis_pipeline(url, table_name, db_url)
    print(result)
