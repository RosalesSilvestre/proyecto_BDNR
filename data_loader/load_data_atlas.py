# --- PASO 1: INSTALAR LIBRERÍAS ---
print("--- Instalando librerías necesarias... ---")
!pip install yfinance pymongo pandas lxml requests -q
print("¡Librerías instaladas!")

# --- PASO 2: IMPORTAR MÓDULOS Y CONFIGURAR ---
import yfinance as yf
import pymongo
from datetime import datetime
import time
import pandas as pd
import requests

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb+srv://<user>:<password>@cluster0.gwoisn4.mongodb.net/?retryWrites=true&w=majority"
DB_NOMBRE = "yfinance_data"
COLECCION_NOMBRE = "acciones_historicas_sp500"
# Definimos cuántos tickers queremos procesar para llegar a ~100k documentos
NUMERO_DE_TICKERS_A_PROCESAR = 80


# --- PASO 3: FUNCIÓN PARA OBTENER TICKERS ---
def obtener_tickers_sp500():
    """
    Obtiene la lista de tickers del S&P 500 desde Wikipedia simulando ser un navegador.
    """
    print("\n--- Obteniendo la lista de tickers del S&P 500... ---")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        tablas = pd.read_html(response.text)
        tabla_sp500 = tablas[0]

        tickers = tabla_sp500['Symbol'].str.replace('.', '-', regex=False).tolist()

        print(f"¡Éxito! Se encontraron {len(tickers)} tickers.")
        return tickers

    except Exception as e:
        print(f"Ocurrió un error al obtener los tickers: {e}")
        return []


# --- PASO 4: FUNCIÓN PRINCIPAL PARA DESCARGAR Y GUARDAR DATOS ---
def procesar_datos():
    """
    Función principal que orquesta la conexión a la base de datos y la recolección de datos.
    """
    lista_de_tickers = obtener_tickers_sp500()

    if not lista_de_tickers:
        print("No hay tickers para procesar. Finalizando el script.")
        return

    print("\n--- Conectando a MongoDB Atlas... ---")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NOMBRE]
        collection = db[COLECCION_NOMBRE]
        client.server_info()
        print(f"¡Conexión exitosa a la colección '{COLECCION_NOMBRE}'!")
    except Exception as e:
        print(f"ERROR: No se pudo conectar a MongoDB Atlas.")
        print(f"Detalle del error: {e}")
        return

    collection.delete_many({})
    print("Colección limpiada para una nueva inserción de datos.")

    documentos_insertados_totales = 0
    tickers_procesados = 0

    # Seleccionamos solo el número de tickers que definimos en la configuración
    tickers_a_procesar = lista_de_tickers[:NUMERO_DE_TICKERS_A_PROCESAR]
    total_tickers = len(tickers_a_procesar)

    print(f"\n--- Iniciando la descarga de datos para {total_tickers} tickers (objetivo ~100k docs)... ---")

    for ticker in tickers_a_procesar:
        tickers_procesados += 1
        print(f"\nProcesando ticker {tickers_procesados}/{total_tickers}: {ticker}")

        try:
            ticker_obj = yf.Ticker(ticker)
            historial = ticker_obj.history(period="5y", auto_adjust=False)

            if historial.empty:
                print(f"No se encontraron datos para {ticker}.")
                time.sleep(1)
                continue

            historial.reset_index(inplace=True)
            documentos_para_insertar = historial.to_dict('records')

            for doc in documentos_para_insertar:
                doc['ticker'] = ticker
                doc['fecha'] = doc.pop('Date').to_pydatetime()
                doc['apertura'] = doc.pop('Open')
                doc['maximo'] = doc.pop('High')
                doc['minimo'] = doc.pop('Low')
                doc['cierre'] = doc.pop('Close')
                doc['cierre_ajustado'] = doc.pop('Adj Close')
                doc['volumen'] = doc.pop('Volume')
                doc['dividendos'] = doc.pop('Dividends')
                doc['division_de_acciones'] = doc.pop('Stock Splits')

            if documentos_para_insertar:
                collection.insert_many(documentos_para_insertar)
                documentos_insertados_totales += len(documentos_para_insertar)
                print(f"Se insertaron {len(documentos_para_insertar)} documentos para {ticker}.")
                print(f"Total de documentos en la DB: {documentos_insertados_totales}")

            time.sleep(1)

        except Exception as e:
            print(f"Ocurrió un error al procesar el ticker {ticker}: {e}")
            time.sleep(1)

    print(f"\n--- Proceso Completado ---")
    docs_finales = collection.count_documents({})
    print(f"Total final de documentos en la colección '{COLECCION_NOMBRE}': {docs_finales}")

# --- EJECUCIÓN DEL SCRIPT ---
if __name__ == "__main__":
    procesar_datos()
