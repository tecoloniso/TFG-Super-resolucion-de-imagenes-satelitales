#Este script descarga un numero N de imagenes desde SENTINEL-2 con una serie de caracteristicas (o filtros)
from datetime import date, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import os
from tqdm import tqdm

# --- CONFIGURACIÓN ---
CREDENTIALS_FILE = 'datasets/credentials.txt'
copernicus_user, copernicus_password = load_credentials(CREDENTIALS_FILE)
if not copernicus_user or not copernicus_password:
    exit()

output_dir = 'datasets/Sentinel_Raw/'
BBOX_X = [-1.830597,42.719777,-1.483154,42.888040]
ft = f'POLYGON(({BBOX_X[0]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[1]}))'
data_collection = "SENTINEL-2"

today =  date.today()
today_string = today.strftime("%Y-%m-%d")
yesterday = today - timedelta(days=100)
yesterday_string = yesterday.strftime("%Y-%m-%d")

MAX_CLOUD_COVER = 20  # Límite máximo de nubes (ej. 20%)
MAX_DOWNLOADS = 1     # Número MÁXIMO de imágenes a descargar
# -------------------------------------

# carga las credenciales desde local, para no leakear mis credenciales :)
def load_credentials(filepath):
    if not os.path.exists(filepath):
        print(f"Error fatal: El archivo de credenciales no se encuentra.")
        print(f"Por favor, crea el archivo en: {filepath}")
        print("Con el formato:\nUSER=tu_usuario\nPASSWORD=tu_contraseña")
        return None, None
    creds = {}
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): continue
            try:
                key, value = line.split('=', 1)
                creds[key.strip()] = value.strip()
            except ValueError:
                pass
    user = creds.get('USER')
    password = creds.get('PASSWORD')
    if not user or not password:
        print(f"Error: 'USER' o 'PASSWORD' no se encontraron en {filepath}")
        return None, None
    print("Credenciales cargadas exitosamente desde el archivo.")
    return user, password

#token de inicio de sesion
def get_keycloak(username: str, password: str) -> str:
    data = {"client_id": "cdse-public", "username": username, "password": password, "grant_type": "password"}
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token", data=data)
        r.raise_for_status()
    except Exception as e:
        raise Exception(f"Keycloak token creation failed. Reponse from the server was: {r.json()}")
    return r.json()["access_token"]


# Query con todos los filtros (buscara imagenes con estas caracteristicas)
query_url = (
    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
    f"$filter=Collection/Name eq '{data_collection}'"
    f" and OData.CSC.Intersects(area=geography'SRID=4326;{ft}')"
    f" and ContentDate/Start gt {yesterday_string}T00:00:00.000Z"
    f" and ContentDate/Start lt {today_string}T00:00:00.000Z"
    f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {MAX_CLOUD_COVER})"
    f"&$count=True&$top=1000"
)

json_ = requests.get(query_url).json()  
p = pd.DataFrame.from_dict(json_["value"])

if p.shape[0] > 0 :
    p["geometry"] = p["GeoFootprint"].apply(shape)
    productDF = gpd.GeoDataFrame(p).set_geometry("geometry")
    productDF = productDF[~productDF["Name"].str.contains("L1C")]
    
    # Ordenar por fecha (mas reciente primero)
    productDF['ContentDate'] = pd.to_datetime(productDF['ContentDate'].apply(lambda x: x['Start']))
    productDF = productDF.sort_values(by='ContentDate', ascending=False)
    
    total_found = len(productDF)
    print(f"Total L2A tiles encontrados que cumplen los criterios: {total_found}")
    
    # Descargar un maximo de imagenes
    if total_found > MAX_DOWNLOADS:
        print(f"Limitando la descarga a las {MAX_DOWNLOADS} imágenes más recientes.")
        productDF = productDF.head(MAX_DOWNLOADS)
    
    allfeat = len(productDF) 

    if allfeat == 0:
        print("No tiles found")
    else:
        ## download all tiles from server
        print(f"--- Iniciando descarga de {allfeat} productos ---")
        
        for index,feat in enumerate(productDF.iterfeatures()):
            
            product_id = feat['properties']['Id']
            product_name = feat['properties']['Name']
            product_identifier = product_name.replace(".SAFE", "")
            output_filename = f"{product_identifier}.zip"
            output_path = os.path.join(output_dir, output_filename)
            
            try:
                # Crear sesión y obtener token
                session = requests.Session()
                keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                
                url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
                response = session.get(url, allow_redirects=False, timeout=30)
                while response.status_code in (301, 302, 303, 307):
                    url = response.headers["Location"]
                    response = session.get(url, allow_redirects=False, timeout=30)
                print(f"Descargando: {product_name} ({index+1}/{allfeat})")
                
                # descargar el archivo EN STREAMING
                with session.get(url, verify=True, allow_redirects=True, stream=True, timeout=30) as file_response:
                    file_response.raise_for_status() 
                    
                    total_size_in_bytes = int(file_response.headers.get('content-length', 0))
                    block_size = 1024 * 8 
                    
                    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, desc=product_name)
                    
                    with open(output_path, "wb") as f:
                        for chunk in file_response.iter_content(chunk_size=block_size):
                            progress_bar.update(len(chunk)) 
                            f.write(chunk) 
                            
                    progress_bar.close()

                if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                    print("Error: La descarga podría estar incompleta.")
                else:
                    print(f"  > Descarga completada: {output_path}")

            except Exception as e:
                print(f"\n¡Ha ocurrido un error descargando {product_name}!")
                print(f"DETALLE DEL ERROR: {e}\n")
            
            
        print(f"\n--- Proceso de descarga finalizado. Se han procesado {allfeat} productos. ---")

else :
    print('no data found')