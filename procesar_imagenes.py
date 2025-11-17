import rasterio
import numpy as np
import cv2 
import lmdb
from PIL import Image
from pathlib import Path
import os
import glob
import zipfile
import tempfile
from tqdm import tqdm
import shutil

# --- CONFIGURACIÓN ---
CARPETA_RAIZ_ZIP = 'datasets/Sentinel_Raw'
CARPETA_SALIDA_LMDB_HR = 'datasets/LMDBs/Reg_X_train_HR.lmdb'
CARPETA_SALIDA_LMDB_LR = 'datasets/LMDBs/Reg_X_train_LR.lmdb'

# Configuración de Normalización
LOW_PERCENTILE = 2
HIGH_PERCENTILE = 98

# Configuración de Parches 
TAMANO_PARCHE_HR = 256     # Tamaño de los parches HR (ej. 256x256)
TAMANO_PARCHE_LR = 64      # Tamaño de los parches LR (ej. 64x64)
FACTOR_ESCALA = 4        # (Debe coincidir 64 * 4 = 256)
SOLAPAMIENTO_HR = 64     # Solapamiento de parches HR (stride=256-64=192)

INTERPOLACION_LR = cv2.INTER_CUBIC # Método para crear la imagen LR
# ---------------------

# Intento evitar instanciar muchas veces la imagen porque ocupa mucha memoria
def normalizar_percentiles(imagen, low_p, high_p):
    # NoData=0, lo ignoramos
    mask = imagen > 0

    # Eliminar valores fuera de los percentiles
    normalizado = np.clip(imagen[mask], low_p, high_p)
    # Normalizar a [0-255]
    normalizado = (((normalizado - low_p) / (high_p - low_p)) * 255).astype(np.uint8)
    
    salida = np.zeros_like(imagen, dtype=np.uint8)
    salida[mask] = normalizado
    return salida


def procesar_zip_a_parches_lmdb(ruta_zip, txn_hr, txn_lr, indice_zip):
    print(f"\nProcesando: {ruta_zip.name}")
    
    # Utilizo tempfile para evitar rutas largas
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            with zipfile.ZipFile(ruta_zip, 'r') as zf:
                # Buscar las 3 bandas que necesitamos
                archivos_bandas = []
                for nombre_archivo in zf.namelist():
                    if 'R10m' in nombre_archivo and (
                        nombre_archivo.endswith('_B02_10m.jp2') or \
                        nombre_archivo.endswith('_B03_10m.jp2') or \
                        nombre_archivo.endswith('_B04_10m.jp2')
                    ):
                        archivos_bandas.append(nombre_archivo)
                
                if len(archivos_bandas) != 3:
                    print(f"  > ERROR: No se encontraron las 3 bandas RGB (B02, B03, B04) en el .zip.")
                    return 0

                for archivo in archivos_bandas:
                    zf.extract(archivo, temp_dir)
                    
        except Exception as e:
            print(f"  > ERROR al leer el .zip: {e}")
            return 0

        # Encontrar las rutas extraídas
        ruta_b02 = next(Path(temp_dir).glob('**/*_B02_10m.jp2')) # Azul
        ruta_b03 = next(Path(temp_dir).glob('**/*_B03_10m.jp2')) # Verde
        ruta_b04 = next(Path(temp_dir).glob('**/*_B04_10m.jp2')) # Rojo

        # Leer bandas (con with para liberar recursos)
        print("  > Leyendo bandas...")
        with rasterio.open(ruta_b04) as src:
            rojo = src.read(1)
        with rasterio.open(ruta_b03) as src:
            verde = src.read(1)
        with rasterio.open(ruta_b02) as src:
            azul = src.read(1)
        
        # Calcular percentiles unificados
        masks = [rojo > 0, verde > 0, azul > 0]
        mascara_combinada = masks[0] & masks[1] & masks[2]
        rgb_apilados_validos = np.concatenate([rojo[mascara_combinada], verde[mascara_combinada], azul[mascara_combinada]])
        
        if rgb_apilados_validos.size == 0:
            print(f"  > ERROR: No se encontraron píxeles válidos (todos son NoData). Saltando.")
            return 0
            
        p_low, p_high = np.percentile(rgb_apilados_validos, (LOW_PERCENTILE, HIGH_PERCENTILE))

        # Normalizar cada banda
        print("  > Normalizando bandas...")
        rojo_norm = normalizar_percentiles(rojo, p_low, p_high)
        verde_norm = normalizar_percentiles(verde, p_low, p_high)
        azul_norm = normalizar_percentiles(azul, p_low, p_high)

        imagen_hr_np = np.stack([rojo_norm, verde_norm, azul_norm], axis=-1)
        # Liberamos todas las variables que no voy a utilizar mas
        del rojo_norm, verde_norm, azul_norm, rojo, verde, azul, rgb_apilados_validos
        
        print(f"  > Imagen HR creada. Dimensiones: {imagen_hr_np.shape}")
        
        # Crear imagen LR
        alto_hr, ancho_hr, _ = imagen_hr_np.shape
        alto_lr = alto_hr // FACTOR_ESCALA
        ancho_lr = ancho_hr // FACTOR_ESCALA
        
        imagen_lr_np = cv2.resize(imagen_hr_np, (ancho_lr, alto_lr), interpolation=INTERPOLACION_LR)
        print(f"  > Imagen LR creada. Dimensiones: {imagen_lr_np.shape}")

        # Cortar en parches y guardar en LMDB
        stride_hr = TAMANO_PARCHE_HR - SOLAPAMIENTO_HR
        
        contador_parches_zip = 0
        
        # Recorro la imagen HR creando los parches 
        for y in range(0, alto_hr - TAMANO_PARCHE_HR + 1, stride_hr):
            for x in range(0, ancho_hr - TAMANO_PARCHE_HR + 1, stride_hr):
                
                # Calcular coordenadas LR correspondientes
                y_lr = y // FACTOR_ESCALA
                x_lr = x // FACTOR_ESCALA
                
                # Extraer parches
                parche_hr = imagen_hr_np[y : y + TAMANO_PARCHE_HR, x : x + TAMANO_PARCHE_HR, :].copy()
                parche_lr = imagen_lr_np[y_lr : y_lr + TAMANO_PARCHE_LR, x_lr : x_lr + TAMANO_PARCHE_LR, :].copy()
                
                # Si mas del 70% de los pixeles en el parche son NoData, elimino el parche
                if np.count(parche_hr == 0) > (0.7 * parche_hr.size())
                    continue
                
                # Guardar en la BD
                clave = f'{indice_zip:03d}_{contador_parches_zip:08d}'.encode('ascii')
                txn_hr.put(clave, parche_hr)
                txn_lr.put(clave, parche_lr)
                
                contador_parches_zip += 1

        print(f"  > {contador_parches_zip} parches útiles guardados")
        del imagen_hr_np, imagen_lr_np
        return contador_parches_zip
    
if __name__ == "__main__":
    
    # Elimino las BD existentes (de otra ejecucion)
    if os.path.exists(CARPETA_SALIDA_LMDB_HR):
        shutil.rmtree(CARPETA_SALIDA_LMDB_HR)
        print(f"Borrada: {CARPETA_SALIDA_LMDB_HR}")
    if os.path.exists(CARPETA_SALIDA_LMDB_LR):
        shutil.rmtree(CARPETA_SALIDA_LMDB_LR)
        print(f"Borrada: {CARPETA_SALIDA_LMDB_LR}")
    
    lista_zips = sorted(list(Path(CARPETA_RAIZ_ZIP).glob('*.zip')))
    if not lista_zips:
        print(f"No se encontró ningún archivo .zip en: {CARPETA_RAIZ_ZIP}")
        exit()
        
    print(f"Se encontraron {len(lista_zips)} archivos .zip para procesar.")
    
    # 2 GB = 2 * 1024^3
    tamano_mapa_db = 2147483648 
    
    env_hr = lmdb.open(CARPETA_SALIDA_LMDB_HR, map_size=tamano_mapa_db, subdir=True)
    env_lr = lmdb.open(CARPETA_SALIDA_LMDB_LR, map_size=tamano_mapa_db, subdir=True)

    contador_total_parches = 0

    with env_hr.begin(write=True) as txn_hr, env_lr.begin(write=True) as txn_lr:
        
        for indice_zip, ruta_zip in enumerate(tqdm(lista_zips, desc="Procesando Zips", unit="archivo")):
            
            try:
                parches_creados = procesar_zip_a_parches_lmdb(ruta_zip, txn_hr, txn_lr, indice_zip)
                if parches_creados:
                    contador_total_parches += parches_creados
            except Exception as e:
                print(f"\nERROR al procesar {ruta_zip.name}")
                print(f"Detalle: {e}")
        
        txn_hr.put(b'num_parches', str(contador_total_parches).encode('ascii'))
        txn_lr.put(b'num_parches', str(contador_total_parches).encode('ascii'))

    env_hr.close()
    env_lr.close()
            
    print(f"\n--- Proceso completado ---")
    print(f"Se han guardado un total de {contador_total_parches} parches (HR y LR)")