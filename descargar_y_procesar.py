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

# Configuración Validación y Test
PORCENTAJE_VALIDACION = 0.2  # % de las imágenes irán a validación
PORCENTAJE_TEST = 0.1        # % de las imágenes irán a test
CARPETA_VAL_HR = 'datasets/Reg_X/val_HR'
CARPETA_VAL_LR = 'datasets/Reg_X/val_LR'
CARPETA_TEST_HR = 'datasets/Reg_X/test_HR'
CARPETA_TEST_LR = 'datasets/Reg_X/test_LR'

# Configurar parches (fijos)
TAMANO_PARCHE_HR = 256
TAMANO_PARCHE_LR = 64
FACTOR_ESCALA = 4
SOLAPAMIENTO_HR = 64

# Data Augmentation
ROTACION_AUGMENTATION = True
ESCALAS_ZOOM = [1.0, 1.5, 2.0, 3.0, 4.0]

# Normalización
VALOR_CORTE_SUPERIOR = 3000  # Valor a partir del cual todo es blanco (Max Reflectancia)
PERCENTILE_MIN = 0.1 # Percentil minimo


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

# Normaliza estirando el histograma min max
def normalizar_rango_dinamico(imagen, valor_min, valor_max):
    # NoData=0, lo ignoramos
    mask = imagen > 0

    # Todo lo menor al mínimo se vuelve el mínimo, todo lo mayor al máximo se vuelve el máximo
    img_clipped = np.clip(imagen[mask], valor_min, valor_max)

    # Escalar a 0-255
    # (Valor - Min) / (Max - Min) * 255
    rango = valor_max - valor_min
    if rango == 0: rango = 1 # Evitar división por cero

    normalizado = ((img_clipped - valor_min) / rango * 255).astype(np.uint8)

    salida = np.zeros_like(imagen, dtype=np.uint8)
    salida[mask] = normalizado
    return salida

# Degradación compleja: Blur -> Resize -> Ruido
def degradar_realista(img_hr):
    # BLUR (Desenfoque)
    sigma = random.uniform(0.5, 1.5)
    img_blur = cv2.GaussianBlur(img_hr, (5, 5), sigma)

    # Resize (bicubica)
    h, w, _ = img_blur.shape
    new_h = h // FACTOR_ESCALA
    new_w = w // FACTOR_ESCALA
    img_lr = cv2.resize(img_blur, (new_w, new_h), interpolation=cv2.INTER_CUBIC)

    # Ruido
    noise_level = random.uniform(2, 10)
    noise = np.random.normal(0, noise_level, img_lr.shape).astype(np.float32)
    img_lr_noisy = img_lr.astype(np.float32) + noise

    # Clip para asegurar que seguimos en rango 0-255 y volver a uint8
    img_lr_final = np.clip(img_lr_noisy, 0, 255).astype(np.uint8)

    return img_lr_final

# Recibe el ZIP en memoria (BytesIO), extrae bandas, crea parches y los guarda
def procesar_buffer(buffer, nombre_producto, contador_global):
    print(f"   > Procesando: {nombre_producto}...")
    parches_guardados = 0
    
    # Usamos un directorio temporal para extraer solo las bandas necesarias
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

            valor_minimo_suelo = np.percentile(rgb_apilados_validos, PERCENTILE_MIN)

            # histograma
            # ---------------------------------------------------------
            try:
                plt.figure(figsize=(10, 6))
                plt.hist(rgb_apilados_validos[::100], bins=100, color='teal', alpha=0.7, label='Distribución Píxeles')

                # Línea ROJA corte fijo VALOR_CORTE_SUPERIOR
                plt.axvline(VALOR_CORTE_SUPERIOR, color='red', linestyle='-', linewidth=2, label=f'Max Fijo ({VALOR_CORTE_SUPERIOR})')

                # Línea AZUL el PERCENTILE_MIN del tile
                plt.axvline(valor_minimo_suelo, color='blue', linestyle='-', linewidth=2, label=f'Min ({int(valor_minimo_suelo)})')

                plt.title(f"Histograma RGB - {nombre_producto}")
                plt.xlabel("Valor Reflectancia (Digital Number)")
                plt.ylabel("Frecuencia")
                plt.legend()
                plt.grid(True, alpha=0.3)

                ruta_hist = os.path.join('temporal', f"{nombre_producto}_HIST.png")
                plt.savefig(ruta_hist)
                plt.close()
                print(f"   > Histograma guardado (Min: {int(valor_minimo_suelo)}).")
            except Exception as e:
                print(f"   > Error generando histograma: {e}")
            # ---------------------------------------------------------


            del rgb_apilados_validos, masks, mascara_combinada
            
            rojo = normalizar_rango_dinamico(rojo, valor_minimo_suelo, VALOR_CORTE_SUPERIOR)
            verde = normalizar_rango_dinamico(verde, valor_minimo_suelo, VALOR_CORTE_SUPERIOR)
            azul = normalizar_rango_dinamico(azul, valor_minimo_suelo, VALOR_CORTE_SUPERIOR)

            # Crear imagen HR completa (H, W, 3)
            img_hr_full = np.stack([rojo, verde, azul], axis=-1)
            del rojo, verde, azul
            
            h_full, w_full, _ = img_hr_full.shape


            # guardar tile
            # ---------------------------------------------------------
            try:
                ruta_full = os.path.join('temporal', f"{nombre_producto}_FULL.png")
                print(f"   > Guardando Tile completo en: {ruta_full}")
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

                        if np.mean(patch_src) < 15: continue # Descartar parches muy oscuros
                        if np.mean(patch_src) > 240: continue # Descartar parches muy blancos

                        # Ajustar la resolucion de la imagen para que independientemente del zoom, sea 256x256
                        if crop_size != TAMANO_PARCHE_HR:
                            patch_hr = cv2.resize(patch_src, (TAMANO_PARCHE_HR, TAMANO_PARCHE_HR), interpolation=cv2.INTER_CUBIC)
                        else:
                            patch_hr = patch_src

                        patch_lr = degradar_realista(patch_hr)

                        # Rotaciones
                        rots = [0, 1, 2, 3] if ROTACION_AUGMENTATION else [0]

                        for k in rots:
                            hr_final = np.rot90(patch_hr, k)
                            lr_final = np.rot90(patch_lr, k)

                            # Formato: IDGlobal_Zoom_Rot.png
                            fname = f"{contador_global:08d}_z{int(zoom*10)}_r{k}.png"

                            Image.fromarray(hr_final).save(os.path.join(CARPETA_SALIDA_HR, fname))
                            Image.fromarray(lr_final).save(os.path.join(CARPETA_SALIDA_LR, fname))

                        contador_global += 4
            
            del img_hr_full
            return contador_global
            
        except Exception as e:
            print(f"   > Error procesando: {e}")
            return contador_global

# Mueve aleatoriamente porcentajes de imágenes de train a val y test simultáneamente.
def generar_splits_datasets():
    porcentaje_total = PORCENTAJE_VALIDACION + PORCENTAJE_TEST
    print(f"\n--- Generando sets de Validación ({PORCENTAJE_VALIDACION*100}%) y Test ({PORCENTAJE_TEST*100}%) ---")

    for d in [CARPETA_VAL_HR, CARPETA_VAL_LR, CARPETA_TEST_HR, CARPETA_TEST_LR]:
        os.makedirs(d, exist_ok=True)

    # Listar todas las imágenes generadas inicialmente en HR (Train)
    archivos_train = [f for f in os.listdir(CARPETA_SALIDA_HR) if f.endswith('.png')]
    total_imgs = len(archivos_train)

    if total_imgs == 0:
        print("   > No hay imágenes para mover.")
        return

    num_val = int(total_imgs * PORCENTAJE_VALIDACION)
    num_test = int(total_imgs * PORCENTAJE_TEST)
    total_a_mover = num_val + num_test

    # Selección conjunta para garantizar que NO hay solapamiento entre Val y Test
    archivos_seleccionados = random.sample(archivos_train, total_a_mover)

    archivos_val = archivos_seleccionados[:num_val]
    archivos_test = archivos_seleccionados[num_val:]

    print(f"   > Total parches: {total_imgs}. Extrayendo {num_val} para VAL y {num_test} para TEST...")

    # Función interna para mover archivos
    def mover_archivos(lista_archivos, carpeta_destino_hr, carpeta_destino_lr):
        movidos = 0
        for nombre_archivo in lista_archivos:
            src_hr = os.path.join(CARPETA_SALIDA_HR, nombre_archivo)
            src_lr = os.path.join(CARPETA_SALIDA_LR, nombre_archivo)
            dst_hr = os.path.join(carpeta_destino_hr, nombre_archivo)
            dst_lr = os.path.join(carpeta_destino_lr, nombre_archivo)

            shutil.move(src_hr, dst_hr)
            if os.path.exists(src_lr):
                shutil.move(src_lr, dst_lr)
            movidos += 1
        return movidos

    movidos_val = mover_archivos(archivos_val, CARPETA_VAL_HR, CARPETA_VAL_LR)
    movidos_test = mover_archivos(archivos_test, CARPETA_TEST_HR, CARPETA_TEST_LR)

    print(f"   > {movidos_val} pares movidos a Validación.")
    print(f"   > {movidos_test} pares movidos a Test.")
    print(f"   > {total_imgs - (movidos_val + movidos_test)} pares restantes en Train.")

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
        generar_splits_datasets()

    print(f"\nTIEMPO TOTAL DE EJECUCIÓN: {(time.time() - tiempo_inicio_total)/60:.2f} minutos")
