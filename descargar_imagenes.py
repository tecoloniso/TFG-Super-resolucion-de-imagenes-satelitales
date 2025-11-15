#Este script descarga un numero N de imagenes desde SENTINEL-2 con una serie de caracteristicas (o filtros)
from datetime import date, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import os
from tqdm import tqdm


# carga las credenciales desde local, para no leakear mis credenciales :)
def cargar_credenciales(ruta_archivo):
    if not os.path.exists(ruta_archivo):
        print(f"Error fatal: El archivo de credenciales no se encuentra.")
        print(f"Crea el archivo en: {ruta_archivo}")
        print("Con el formato:\nUSER=tu_usuario\nPASSWORD=tu_contraseña")
        return None, None
    credenciales = {}
    with open(ruta_archivo, 'r') as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith('#'): continue
            try:
                clave, valor = linea.split('=', 1)
                credenciales[clave.strip()] = valor.strip()
            except ValueError:
                pass
    usuario = credenciales.get('USER')
    clave = credenciales.get('PASSWORD')
    if not usuario or not clave:
        print(f"Error: 'USER' o 'PASSWORD' no se encontraron en {ruta_archivo}")
        return None, None
    print("Credenciales cargadas exitosamente desde el archivo.")
    return usuario, clave

#token de inicio de sesion
def obtener_token_keycloak(usuario: str, clave: str) -> str:
    datos = {"client_id": "cdse-public", "username": usuario, "password": clave, "grant_type": "password"}
    try:
        respuesta = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token", data=datos)
        respuesta.raise_for_status()
    except Exception as e:
        raise Exception(f"Fallo al crear el token de Keycloak. La respuesta del servidor fue: {respuesta.json()}")
    return respuesta.json()["access_token"]


# --- CONFIGURACIÓN ---
ARCHIVO_CREDENCIALES = 'datasets/credentials.txt'
usuario_copernicus, clave_copernicus = cargar_credenciales(ARCHIVO_CREDENCIALES)
if not usuario_copernicus or not clave_copernicus:
    exit()

directorio_salida = 'datasets/Sentinel_Raw/'
BBOX_X = [-1.830597,42.719777,-1.483154,42.888040]
huella_wkt = f'POLYGON(({BBOX_X[0]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[1]}))'
coleccion_datos = "SENTINEL-2"

fecha_fin =  date.today()
fecha_fin_texto = fecha_fin.strftime("%Y-%m-%d")
fecha_inicio = fecha_fin - timedelta(days=100)
fecha_inicio_texto = fecha_inicio.strftime("%Y-%m-%d")

MAX_NUBES_PORCENTAJE = 90  # Limite maximo de nubes (ej. 20%)
MAX_DESCARGAS = 1      # Número maximo de imágenes a descargar
# -----------------------------------


# Query con todos los filtros (buscara imagenes con estas caracteristicas)
url_consulta = (
    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
    f"$filter=Collection/Name eq '{coleccion_datos}'"
    f" and OData.CSC.Intersects(area=geography'SRID=4326;{huella_wkt}')"
    f" and ContentDate/Start gt {fecha_inicio_texto}T00:00:00.000Z"
    f" and ContentDate/Start lt {fecha_fin_texto}T00:00:00.000Z"
    f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {MAX_NUBES_PORCENTAJE})"
    f"&$count=True&$top=1000"
)

respuesta_json = requests.get(url_consulta).json()  
df_temporal = pd.DataFrame.from_dict(respuesta_json["value"])

if df_temporal.shape[0] > 0 :
    df_temporal["geometry"] = df_temporal["GeoFootprint"].apply(shape)
    df_productos = gpd.GeoDataFrame(df_temporal).set_geometry("geometry")
    df_productos = df_productos[~df_productos["Name"].str.contains("L1C")]
    
    # Ordenar por fecha (mas reciente primero)
    df_productos['ContentDate'] = pd.to_datetime(df_productos['ContentDate'].apply(lambda x: x['Start']))
    df_productos = df_productos.sort_values(by='ContentDate', ascending=False)
    
    total_encontrados = len(df_productos)
    print(f"Total L2A tiles encontrados que cumplen los criterios: {total_encontrados}")
    
    # Descargar un maximo de imagenes
    if total_encontrados > MAX_DESCARGAS:
        print(f"Limitando la descarga a las {MAX_DESCARGAS} imágenes más recientes.")
        df_productos = df_productos.head(MAX_DESCARGAS)
    
    total_a_descargar = len(df_productos) 

    if total_a_descargar == 0:
        print("No se encontraron 'tiles' que cumplan los filtros")
    else:
        # Descargar las imagenes
        print(f"--- Iniciando descarga de {total_a_descargar} productos ---")
        
        for indice, producto in enumerate(df_productos.iterfeatures()):
            
            id_producto = producto['properties']['Id']
            nombre_producto = producto['properties']['Name']
            identificador_producto = nombre_producto.replace(".SAFE", "")
            nombre_archivo_salida = f"{identificador_producto}.zip"
            ruta_salida = os.path.join(directorio_salida, nombre_archivo_salida)
            
            if os.path.exists(ruta_salida):
                print(f"El archivo ya existe, se omite la descarga: {nombre_archivo_salida}")
            else:
                try:
                    # Crear sesión y obtener token
                    sesion = requests.Session()
                    token_keycloak = obtener_token_keycloak(usuario_copernicus, clave_copernicus)
                    sesion.headers.update({"Authorization": f"Bearer {token_keycloak}"})
                    
                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({id_producto})/$value"
                    respuesta = sesion.get(url, allow_redirects=False, timeout=30)
                    while respuesta.status_code in (301, 302, 303, 307):
                        url = respuesta.headers["Location"]
                        respuesta = sesion.get(url, allow_redirects=False, timeout=30)
                    print(f"Descargando: {nombre_producto} ({indice+1}/{total_a_descargar})")
                    
                    # descargar el archivo EN STREAMING
                    with sesion.get(url, verify=True, allow_redirects=True, stream=True, timeout=30) as respuesta_archivo:
                        respuesta_archivo.raise_for_status() 
                        
                        tamano_total_bytes = int(respuesta_archivo.headers.get('content-length', 0))
                        tamano_bloque = 1024 * 8 
                        
                        barra_progreso = tqdm(total=tamano_total_bytes, unit='iB', unit_scale=True, desc=nombre_producto)
                        
                        with open(ruta_salida, "wb") as f:
                            for trozo in respuesta_archivo.iter_content(chunk_size=tamano_bloque):
                                barra_progreso.update(len(trozo)) 
                                f.write(trozo) 
                                
                        barra_progreso.close()

                    if tamano_total_bytes != 0 and barra_progreso.n != tamano_total_bytes:
                        print("Error: La descarga podría estar incompleta.")
                    else:
                        print(f"   > Descarga completada: {ruta_salida}")

                except Exception as e:
                    print(f"\n¡Ha ocurrido un error descargando {nombre_producto}!")
                    print(f"DETALLE DEL ERROR: {e}\n")
            
            
        print(f"\n--- Proceso de descarga finalizado. Se han procesado {total_a_descargar} productos. ---")

else :
    print('no data found')