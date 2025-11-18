import rasterio
import numpy as np
import cv2  # Importamos OpenCV para el reescalado (resize)
from PIL import Image
from pathlib import Path
import os
import glob
import zipfile
import tempfile
from tqdm import tqdm  # Para la barra de progreso del bucle principal
import shutil  # Para borrar carpetas antiguas

# --- CONFIGURACIÓN ---
CARPETA_RAIZ_ZIP = 'datasets/Sentinel_Raw/val_zips' 

# ¡NUEVAS CARPETAS DE SALIDA! Ahora son carpetas normales para parches.
CARPETA_SALIDA_PATCHES_HR = 'datasets/Reg_X/val_HR'
CARPETA_SALIDA_PATCHES_LR = 'datasets/Reg_X/val_LR'

# Configuración de Normalización
LOW_PERCENTILE = 2
HIGH_PERCENTILE = 98

# Configuración de Parches 
TAMANO_PARCHE_HR = 256
TAMANO_PARCHE_LR = 64
FACTOR_ESCALA = 4
SOLAPAMIENTO_HR = 64
INTERPOLACION_LR = cv2.INTER_CUBIC
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


def procesar_zip_a_parches(ruta_zip, indice_zip):
    print(f"\nProcesando: {ruta_zip.name}")
    
    contador_parches_zip = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # (El código de extracción de bandas es idéntico)
        try:
            with zipfile.ZipFile(ruta_zip, 'r') as zf:
                archivos_bandas = []
                for nombre_archivo in zf.namelist():
                    if 'R10m' in nombre_archivo and (
                        nombre_archivo.endswith('_B02_10m.jp2') or \
                        nombre_archivo.endswith('_B03_10m.jp2') or \
                        nombre_archivo.endswith('_B04_10m.jp2')
                    ):
                        archivos_bandas.append(nombre_archivo)
                if len(archivos_bandas) != 3:
                    print(f"  > ERROR: No se encontraron las 3 bandas RGB. Saltando zip.")
                    return 0
                for archivo in archivos_bandas:
                    zf.extract(archivo, temp_dir)
        except Exception as e:
            print(f"  > ERROR al leer el .zip: {e}")
            return 0

        ruta_b02 = next(Path(temp_dir).glob('**/*_B02_10m.jp2'))
        ruta_b03 = next(Path(temp_dir).glob('**/*_B03_10m.jp2'))
        ruta_b04 = next(Path(temp_dir).glob('**/*_B04_10m.jp2'))

        print("  > Leyendo y normalizando bandas...")
        with rasterio.open(ruta_b04) as src: rojo = src.read(1)
        with rasterio.open(ruta_b03) as src: verde = src.read(1)
        with rasterio.open(ruta_b02) as src: azul = src.read(1)
        
        masks = [rojo > 0, verde > 0, azul > 0]
        mascara_combinada = masks[0] & masks[1] & masks[2]
        rgb_apilados_validos = np.concatenate([rojo[mascara_combinada], verde[mascara_combinada], azul[mascara_combinada]])
        if rgb_apilados_validos.size == 0:
            print(f"  > ERROR: No se encontraron píxeles válidos. Saltando zip.")
            return 0
            
        p_low, p_high = np.percentile(rgb_apilados_validos, (LOW_PERCENTILE, HIGH_PERCENTILE))
        del rgb_apilados_validos

        rojo_norm = normalizar_percentiles(rojo, p_low, p_high)
        verde_norm = normalizar_percentiles(verde, p_low, p_high)
        azul_norm = normalizar_percentiles(azul, p_low, p_high)
        imagen_hr_np = np.stack([rojo_norm, verde_norm, azul_norm], axis=-1)
        del rojo_norm, verde_norm, azul_norm, rojo, verde, azul, masks
        
        alto_hr, ancho_hr, _ = imagen_hr_np.shape
        alto_lr = alto_hr // FACTOR_ESCALA
        ancho_lr = ancho_hr // FACTOR_ESCALA
        
        imagen_lr_np = cv2.resize(imagen_hr_np, (ancho_lr, alto_lr), interpolation=INTERPOLACION_LR)

        print("  > Cortando y guardando parches PNG...")
        stride_hr = TAMANO_PARCHE_HR - SOLAPAMIENTO_HR
        
        for y in range(0, alto_hr - TAMANO_PARCHE_HR + 1, stride_hr):
            for x in range(0, ancho_hr - TAMANO_PARCHE_HR + 1, stride_hr):
                
                y_lr, x_lr = y // FACTOR_ESCALA, x // FACTOR_ESCALA
                
                parche_hr_np = imagen_hr_np[y:y+TAMANO_PARCHE_HR, x:x+TAMANO_PARCHE_HR, :].copy()
                
                if np.mean(parche_hr_np) < 5: # Omitir parches vacíos
                    continue
                    
                parche_lr_np = imagen_lr_np[y_lr:y_lr+TAMANO_PARCHE_LR, x_lr:x_lr+TAMANO_PARCHE_LR, :].copy()
                
                # Crear clave única (ahora es el nombre del archivo)
                nombre_archivo_parche = f'{indice_zip:03d}_{contador_parches_zip:08d}.png'
                
                # --- ¡NUEVO MÉTODO DE GUARDADO! ---
                try:
                    # Guardar parche HR
                    ruta_salida_hr = Path(CARPETA_SALIDA_PATCHES_HR) / nombre_archivo_parche
                    Image.fromarray(parche_hr_np, 'RGB').save(ruta_salida_hr)
                    
                    # Guardar parche LR
                    ruta_salida_lr = Path(CARPETA_SALIDA_PATCHES_LR) / nombre_archivo_parche
                    Image.fromarray(parche_lr_np, 'RGB').save(ruta_salida_lr)
                    
                    contador_parches_zip += 1
                except Exception as e:
                    print(f"Error guardando parche: {e}")

        print(f"  > {contador_parches_zip} parches útiles guardados")
        del imagen_hr_np, imagen_lr_np
        return contador_parches_zip
    
if __name__ == "__main__":
    
    # ¡Limpiamos las carpetas de PARCHES!
    print("Limpiando carpetas de parches antiguas (si existen)...")
    if os.path.exists(CARPETA_SALIDA_PATCHES_HR):
        shutil.rmtree(CARPETA_SALIDA_PATCHES_HR)
        print(f"Borrada: {CARPETA_SALIDA_PATCHES_HR}")
    if os.path.exists(CARPETA_SALIDA_PATCHES_LR):
        shutil.rmtree(CARPETA_SALIDA_PATCHES_LR)
        print(f"Borrada: {CARPETA_SALIDA_PATCHES_LR}")
    print("Limpieza completada.")
    
    # Asegurarse de que las carpetas de salida existan
    os.makedirs(CARPETA_SALIDA_PATCHES_HR, exist_ok=True)
    os.makedirs(CARPETA_SALIDA_PATCHES_LR, exist_ok=True)
    
    lista_zips = sorted(list(Path(CARPETA_RAIZ_ZIP).glob('*.zip')))
    if not lista_zips:
        print(f"No se encontró ningún archivo .zip en: {CARPETA_RAIZ_ZIP}")
        exit()
        
    print(f"Se encontraron {len(lista_zips)} archivos .zip para procesar.")
    
    contador_total_parches = 0

    for indice_zip, ruta_zip in enumerate(tqdm(lista_zips, desc="Procesando Zips", unit="archivo")):
        try:
            parches_creados = procesar_zip_a_parches(ruta_zip, indice_zip)
            if parches_creados:
                contador_total_parches += parches_creados
        except Exception as e:
            print(f"\nERROR al procesar {ruta_zip.name}\nDetalle: {e}")
            
    print(f"\n--- Proceso completado ---")
    print(f"Se han guardado un total de {contador_total_parches} parches PNG en:")