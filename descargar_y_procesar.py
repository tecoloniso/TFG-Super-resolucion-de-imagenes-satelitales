import matplotlib.pyplot as plt
import io
import os
import shutil
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import tempfile
import zipfile
import requests
import numpy as np
import cv2
import rasterio
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from PIL import Image
from pathlib import Path
from datetime import date, timedelta
import time


ARCHIVO_CREDENCIALES = 'datasets/credentials.txt'

# Area de interes (BBOX)
# http://bboxfinder.com/
BBOX_X = [-2.636719,41.857288,-0.708618,43.333169]
HUELLA_WKT = f'POLYGON(({BBOX_X[0]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[1]}, {BBOX_X[2]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[3]}, {BBOX_X[0]} {BBOX_X[1]}))'
COLECCION = "SENTINEL-2"

# Fechas
DIAS = 40000 # x ultimos dias
FECHA_FIN = date.today() #- timedelta(days=25)
FECHA_INICIO = FECHA_FIN - timedelta(days=DIAS)

# Filtros
MAX_NUBES = 20      # % de nubes de las imagenes
MAX_IMAGENES = 20    # Cuantas imagenes procesar

# Carpetas donde guardar los parches
CARPETA_SALIDA_HR = 'datasets/Reg_X/train_HR'
CARPETA_SALIDA_LR = 'datasets/Reg_X/train_LR'

# Configuración Validación
PORCENTAJE_VALIDACION = 0.2  # % de las imágenes irán a validación
CARPETA_VAL_HR = 'datasets/Reg_X/val_HR'
CARPETA_VAL_LR = 'datasets/Reg_X/val_LR'

# Configurar parches (fijos)
TAMANO_PARCHE_HR = 256
TAMANO_PARCHE_LR = 64
FACTOR_ESCALA = 4
SOLAPAMIENTO_HR = 64
INTERPOLACION_LR = cv2.INTER_CUBIC # Degradación bicúbica para el LR

# Data Augmentation
ROTACION_AUGMENTATION = False#True
ESCALAS_ZOOM = [32.0]#[1.0, 1.5, 2.0, 3, 4]

# Normalización
VALOR_CORTE_SUPERIOR = 3000  # Valor a partir del cual todo es blanco (Max Reflectancia)


# carga las credenciales desde local, para no leakear mis credenciales :)
def cargar_credenciales(ruta_archivo):
    if not os.path.exists(ruta_archivo):
        print(f"Error: No existe {ruta_archivo}")
        print("Formato:\nUSER=tu_usuario\nPASSWORD=tu_contraseña")
        return None, None
    creds = {}
    with open(ruta_archivo, 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                creds[k.strip()] = v.strip()
    return creds.get('USER'), creds.get('PASSWORD')

def obtener_token(usuario, clave):
    data = {"client_id": "cdse-public", "username": usuario, "password": clave, "grant_type": "password"}
    r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token", data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# Normaliza usando un valor tope fijo
def normalizar_fijo(imagen, valor_corte):
    # NoData=0, lo ignoramos
    mask = imagen > 0

    # Todo lo que supere 3000 se queda en 3000
    img_clipped = np.clip(imagen[mask], 0, valor_corte)

    # Escalar a 0-255
    # (Valor / 3000) * 255
    normalizado = (img_clipped / valor_corte * 255).astype(np.uint8)

    salida = np.zeros_like(imagen, dtype=np.uint8)
    salida[mask] = normalizado
    return salida

# Recibe el ZIP en memoria (BytesIO), extrae bandas, crea parches y los guarda
def procesar_buffer(buffer, nombre_producto, contador_global):
    print(f"   > Procesando: {nombre_producto}...")
    parches_guardados = 0
    
    # Usamos un directorio temporal para extraer SOLO las bandas necesarias
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            with zipfile.ZipFile(buffer) as zf:
                archivos_bandas = []
                for name in zf.namelist():
                    # Buscamos resolucion 10m y bandas B02, B03, B04
                    if 'R10m' in name and name.endswith(('_B02_10m.jp2', '_B03_10m.jp2', '_B04_10m.jp2')):
                        archivos_bandas.append(name)
                
                if len(archivos_bandas) != 3:
                    print("   > Error: No se encontraron las 3 bandas RGB.")
                    return 0
                
                for archivo in archivos_bandas:
                    zf.extract(archivo, temp_dir)
                    

            ruta_b04 = next(Path(temp_dir).glob('**/*_B04_10m.jp2')) # Rojo
            ruta_b03 = next(Path(temp_dir).glob('**/*_B03_10m.jp2')) # Verde
            ruta_b02 = next(Path(temp_dir).glob('**/*_B02_10m.jp2')) # Azul
            
            with rasterio.open(ruta_b04) as src: rojo = src.read(1)
            with rasterio.open(ruta_b03) as src: verde = src.read(1)
            with rasterio.open(ruta_b02) as src: azul = src.read(1)

            masks = [rojo > 0, verde > 0, azul > 0]
            mascara_combinada = masks[0] & masks[1] & masks[2]
            rgb_apilados_validos = np.concatenate([rojo[mascara_combinada], verde[mascara_combinada], azul[mascara_combinada]])
            
            if rgb_apilados_validos.size == 0:
                print(f"  > ERROR: No se encontraron píxeles válidos. Saltando tile")
                return 0


            # histograma
            # ---------------------------------------------------------
            try:
                plt.figure(figsize=(10, 6))
                plt.hist(rgb_apilados_validos[::100], bins=100, color='teal', alpha=0.7, label='Distribución Píxeles')

                # Línea roja fija en 3000
                plt.axvline(VALOR_CORTE_SUPERIOR, color='red', linestyle='-', linewidth=2, label=f'Corte Fijo ({VALOR_CORTE_SUPERIOR})')

                plt.title(f"Histograma RGB - {nombre_producto}")
                plt.xlabel("Valor Reflectancia (Digital Number)")
                plt.ylabel("Frecuencia")
                plt.legend()
                plt.grid(True, alpha=0.3)

                ruta_hist = os.path.join('temporal', f"{nombre_producto}_HIST.png")
                plt.savefig(ruta_hist)
                plt.close()
                print(f"   > Histograma guardado.")
            except Exception as e:
                print(f"   > Error generando histograma: {e}")

            # Liberamos memoria del histograma
            del rgb_apilados_validos, masks, mascara_combinada
            # ---------------------------------------------------------

                

            del rgb_apilados_validos, masks, mascara_combinada
            
            rojo = normalizar_fijo(rojo, VALOR_CORTE_SUPERIOR)
            verde = normalizar_fijo(verde, VALOR_CORTE_SUPERIOR)
            azul = normalizar_fijo(azul, VALOR_CORTE_SUPERIOR)

            # Crear imagen HR completa (H, W, 3)
            img_hr_full = np.stack([rojo, verde, azul], axis=-1)
            del rojo, verde, azul
            
            h_full, w_full, _ = img_hr_full.shape


            # guardar tile
            # ---------------------------------------------------------
            try:
                ruta_full = os.path.join('temporal', f"{nombre_producto}_FULL.png")
                print(f"   > Guardando Tile completo en: {ruta_full}")
                # optimize=True ayuda a reducir el peso del PNG sin perder calidad visible
                Image.fromarray(img_hr_full).save(ruta_full, optimize=True)
            except Exception as e:
                print(f"   > No se pudo guardar el tile completo: {e}")
            # ---------------------------------------------------------


            # Generar parches
            for zoom in ESCALAS_ZOOM:
                print(f"  > Procesando zoom: {zoom}")
                # Calculamos qué tamaño de trozo tenemos que cortar de la imagen original para que, al reducirlo, quede de 256x256
                crop_size = int(TAMANO_PARCHE_HR * zoom)

                # Ajustamos el paso para que el solapamiento sea proporcional
                paso = crop_size - int(SOLAPAMIENTO_HR * zoom)
                if paso < 1: paso = 1

                for y in range(0, h_full - crop_size + 1, paso):
                    for x in range(0, w_full - crop_size + 1, paso):

                        # Recortar sobre la imagen original
                        patch_src = img_hr_full[y:y+crop_size, x:x+crop_size, :].copy()

                        if np.mean(patch_src) < 10: continue # Descartar parches oscuros

                        # Ajustar la resolucion de la imagen para que independientemente del zoom, sea 256x256
                        if crop_size != TAMANO_PARCHE_HR:
                            patch_hr = cv2.resize(patch_src, (TAMANO_PARCHE_HR, TAMANO_PARCHE_HR), interpolation=cv2.INTER_CUBIC)
                        else:
                            patch_hr = patch_src

                        patch_lr = cv2.resize(patch_hr, (TAMANO_PARCHE_LR, TAMANO_PARCHE_LR), interpolation=INTERPOLACION_LR)

                        # Rotaciones
                        rots = [0, 1, 2, 3] if ROTACION_AUGMENTATION else [0]

                        for k in rots:
                            hr_final = np.rot90(patch_hr, k)
                            lr_final = np.rot90(patch_lr, k)

                            # Formato: IDGlobal_Zoom_Rot.png
                            fname = f"{contador_global:08d}_z{int(zoom*10)}_r{k}.png"

                            Image.fromarray(hr_final).save(os.path.join(CARPETA_SALIDA_HR, fname))
                            Image.fromarray(lr_final).save(os.path.join(CARPETA_SALIDA_LR, fname))

                        contador_global += 1
            
            del img_hr_full
            return contador_global * len(rots)
            
        except Exception as e:
            print(f"   > Error procesando: {e}")
            return contador_global * len(rots)

# Mueve aleatoriamente un porcentaje de imágenes de train a val.
def generar_split_validacion():
    print(f"\n--- Generando set de validación ({PORCENTAJE_VALIDACION*100}%) ---")

    # Crear directorios
    os.makedirs(CARPETA_VAL_HR, exist_ok=True)
    os.makedirs(CARPETA_VAL_LR, exist_ok=True)

    # Listar todas las imágenes generadas en HR
    archivos_train = [f for f in os.listdir(CARPETA_SALIDA_HR) if f.endswith('.png')]
    total_imgs = len(archivos_train)

    if total_imgs == 0:
        print("   > No hay imágenes para mover.")
        return

    num_a_mover = int(total_imgs * PORCENTAJE_VALIDACION)
    archivos_seleccionados = random.sample(archivos_train, num_a_mover)

    print(f"   > Moviendo {num_a_mover} imágenes a carpetas de validación...")

    movidos = 0
    for nombre_archivo in archivos_seleccionados:
        # Rutas Origen
        src_hr = os.path.join(CARPETA_SALIDA_HR, nombre_archivo)
        src_lr = os.path.join(CARPETA_SALIDA_LR, nombre_archivo)

        # Rutas Destino
        dst_hr = os.path.join(CARPETA_VAL_HR, nombre_archivo)
        dst_lr = os.path.join(CARPETA_VAL_LR, nombre_archivo)

        # Mover HR
        shutil.move(src_hr, dst_hr)

        # Mover LR
        if os.path.exists(src_lr):
            shutil.move(src_lr, dst_lr)

        movidos += 1
    print(f"   > {movidos} pares de imágenes movidos a validación.")


if __name__ == "__main__":
    tiempo_inicio_total = time.time()

    for d in [CARPETA_SALIDA_HR, CARPETA_SALIDA_LR, 'temporal']:
        if os.path.exists(d): shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    user, password = cargar_credenciales(ARCHIVO_CREDENCIALES)
    if not user: exit()
    
    print("--- Buscando imágenes en Copernicus ---")
    
    # Consulta OData
    url = (f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
           f"$filter=Collection/Name eq '{COLECCION}'"
           f" and OData.CSC.Intersects(area=geography'SRID=4326;{HUELLA_WKT}')"
           f" and contains(Name,'MSIL2A')" # filtro L2A
           f" and ContentDate/Start gt {FECHA_INICIO}T00:00:00.000Z"
           f" and ContentDate/Start lt {FECHA_FIN}T00:00:00.000Z"
           f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {MAX_NUBES})"
           f"&$count=True&$top={MAX_IMAGENES}&$orderby=ContentDate/Start desc")
    
    try:
        #resp = requests.get(url).json()
        resp = requests.get(url)
        print(resp)
        resp = resp.json()
        products = pd.DataFrame.from_dict(resp.get("value", []))
    except Exception as e:
        print(f"Error conectando con Copernicus: {e}")
        exit()

    if products.empty:
        print("No se encontraron imágenes con esos filtros.")
        exit()
        

    print(f"Encontradas {len(products)} imágenes L2A")
    contador_global = 0
    session = requests.Session()
    
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[408, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    for idx, row in products.iterrows():
        try:
            tiempo_inicio_tile = time.time()

            prod_id = row['Id']
            prod_name = row['Name']

            # Renovar el token de sesion cada tile
            token = obtener_token(user, password)
            session.headers.update({"Authorization": f"Bearer {token}"})


            print(f"\n[{idx+1}/{len(products)}] Descargando: {prod_name} ...")
            down_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({prod_id})/$value"
            
            r = session.get(down_url, allow_redirects=False)
            while r.status_code in (301, 302, 303, 307):
                down_url = r.headers['Location']
                r = session.get(down_url, allow_redirects=False)
            
            r_file = session.get(down_url, verify=True)
            r_file.raise_for_status()

            # Convertir los bytes descargados en un archivo en memoria
            buffer = io.BytesIO(r_file.content)
            contador_anterior = contador_global
            contador_global = procesar_buffer(buffer, prod_name, contador_global)

            buffer.close()
            del buffer

            print(f"   > Generados {contador_global - contador_anterior} parches de esta imagen.")
            print(f"   > Tiempo total para esta imagen: {(time.time() - tiempo_inicio_tile)/60:.1f} minutos")
            print(f"   > Tiempo total hasta ahora: {(time.time() - tiempo_inicio_total)/60:.1f} minutos")

            
        except Exception as e:
            print(f"Error general: {e}")


    print(f"Total parches generados: {contador_global}")
    print(f"Guardados en: {CARPETA_SALIDA_HR}")

    if contador_global > 0:
        generar_split_validacion()

    print(f"\nTIEMPO TOTAL DE EJECUCIÓN: {(time.time() - tiempo_inicio_total)/60:.2f} minutos")
