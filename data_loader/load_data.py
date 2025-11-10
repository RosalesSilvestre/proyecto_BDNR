import os
import time
import requests
import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# --- CONFIGURACIÓN ---
DB_NAME = "yfinance_data"
COLLECTION_NAME = "acciones_historicas_sp500"
TICKERS_TO_PROCESS = 80
DATA_PERIOD = "5y"

def get_sp500_tickers():
    # Esta funcion regresa la lista de tickers que utilizamos para subir a la base de datos
    print("--- Obteniendo la lista de tickers del S&P 500...")
    try:
        #Aqui hacemos las solicitudes http
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        tickers = tables[0]['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f" ¡Éxito! Se encontraron {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        print(f" Error al obtener los tickers: {e}")
        return []

def fetch_stock_data():
    # esta funcion procesa los datos de los ticker con ayuda de la libreria yfinance
    tickers = get_sp500_tickers()
    if not tickers: return None
    
    tickers_to_process = tickers[:TICKERS_TO_PROCESS]
    all_documents = []
    print(f"\n--- Iniciando la descarga de datos para {len(tickers_to_process)} tickers...")
    for i, ticker in enumerate(tickers_to_process):
        print(f"Procesando ticker {i + 1}/{len(tickers_to_process)}: {ticker}")
        try:
            ticker_obj = yf.Ticker(ticker)
            history = ticker_obj.history(period=DATA_PERIOD, auto_adjust=False)
            if history.empty: continue
            history.reset_index(inplace=True)
            documents_to_insert = history.to_dict('records')
            for doc in documents_to_insert:
                doc['ticker'] = ticker
                doc['fecha'] = doc.pop('Date').to_pydatetime()
            all_documents.extend(documents_to_insert)
            time.sleep(1)
        except Exception as e:
            print(f"    -  Error procesando {ticker}: {e}")
    return all_documents


def load_data_to_mongo(data):
    #Esta funcion se conecta a mongodb local y agrega los datos.
    if not data:
        print("No hay datos para cargar.")
        return

    # Obtener credenciales del entorno
    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_pass = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = "mongodb" # 'mongodb' es el nombre del servicio

    # Construir la URI para un contenedor local, no para Atlas
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/"
    
    print(f"\n--- Intentando conectar a MongoDB en {mongo_host}...")
    
    max_retries = 5
    retry_delay = 10
    for attempt in range(max_retries):
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ismaster')
            print(" Conexión a MongoDB exitosa.")
            
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            print("Limpiando la colección...")
            collection.delete_many({})
            
            print(f"Insertando {len(data)} documentos...")
            collection.insert_many(data)
            
            print(f"¡Éxito! Se insertaron {len(data)} documentos.")
            client.close()
            return
        except ConnectionFailure:
            print(f"Intento {attempt + 1}/{max_retries}: Falló la conexión. Reintentando...")
            time.sleep(retry_delay)
    
    print(" No se pudo conectar a MongoDB. Saliendo.")

if __name__ == "__main__":
    financial_data = fetch_stock_data()
    if financial_data:
        load_data_to_mongo(financial_data)
