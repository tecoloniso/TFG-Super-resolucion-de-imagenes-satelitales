import rasterio
import numpy as np
import cv2  # Para el reescalado (resize)
from PIL import Image
from pathlib import Path
import os
import glob
import zipfile
import tempfile
from tqdm import tqdm  # Para la barra de progreso del bucle principal
import shutil  # Para borrar carpetas antiguas

# --- CONFIGURACIÓN ---
CARPETA_RAIZ_ZIP = 'datasets/Sentinel_Raw/train_zips' 

# Carpetas de salida
CARPETA_SALIDA_PATCHES_HR = 'datasets/Reg_X/train_HR'
CARPETA_SALIDA_PATCHES_LR = 'datasets/Reg_X/train_LR'

# Configuración de Normalización
LOW_PERCENTILE = 2
HIGH_PERCENTILE = 98

# Parches
TAMANO_PARCHE_HR = 256    # Tamaño final que entra a la red (HR)
TAMANO_PARCHE_LR = 64     # Tamaño final LR (HR / FACTOR_ESCALA)
FACTOR_ESCALA = 4         # Relación de super-resolución
SOLAPAMIENTO_HR = 64      # Solapamiento base (se ajustará con el zoom)
INTERPOLACION_LR = cv2.INTER_CUBIC

# AUMENTACION DE DATOS
# Rotacion
ROTACION_AUGMENTATION = True 
# Multi zoom (numero de zoom mas grande --> se ve una region mas grande)
ESCALAS_ZOOM = [1.0, 1.5, 2.0] 
# -----------------------------------------------------

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

        # Buscar archivos extraídos
        try:
            ruta_b02 = next(Path(temp_dir).glob('**/*_B02_10m.jp2'))
            ruta_b03 = next(Path(temp_dir).glob('**/*_B03_10m.jp2'))
            ruta_b04 = next(Path(temp_dir).glob('**/*_B04_10m.jp2'))
        except StopIteration:
            print("  > ERROR: No se pudieron localizar los archivos .jp2 extraídos.")
            return 0

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
        
        # Imagen gigante completa en memoria (H, W, 3)
        imagen_grande_hr = np.stack([rojo_norm, verde_norm, azul_norm], axis=-1)
        del rojo_norm, verde_norm, azul_norm, rojo, verde, azul, masks
        
        alto_grande, ancho_grande, _ = imagen_grande_hr.shape

        print(f"  > Generando parches con Zoom {ESCALAS_ZOOM} y Rotación={ROTACION_AUGMENTATION}...")

        for escala_zoom in ESCALAS_ZOOM:
            print(f"  > Procesando zoom: {escala_zoom}")
            # Calculamos qué tamaño de trozo tenemos que cortar de la imagen original para que, al reducirlo, quede de 256x256
            tamano_recorte_origen = int(TAMANO_PARCHE_HR * escala_zoom)
            
            # Ajustamos el stride para que el solapamiento sea proporcional
            stride = tamano_recorte_origen - int(SOLAPAMIENTO_HR * escala_zoom)
            if stride < 1: stride = 1

            # Recorremos la imagen gigante
            for y in range(0, alto_grande - tamano_recorte_origen + 1, stride):
                for x in range(0, ancho_grande - tamano_recorte_origen + 1, stride):
                    
                    # Recortar sobre la imagen original
                    parche_src = imagen_grande_hr[y:y+tamano_recorte_origen, x:x+tamano_recorte_origen, :].copy()
                    
                    # Comprobar si es un parche valido (no negro)
                    if np.mean(parche_src) < 5: 
                        continue
                    
                    # Ajustar la resolucion de la imagen para que independientemente del zoom, sea 256x256
                    if tamano_recorte_origen != TAMANO_PARCHE_HR:
                        parche_hr_final = cv2.resize(parche_src, (TAMANO_PARCHE_HR, TAMANO_PARCHE_HR), interpolation=cv2.INTER_CUBIC)
                    else:
                        parche_hr_final = parche_src

                    # Crear el par LR 64x64
                    parche_lr_final = cv2.resize(parche_hr_final, (TAMANO_PARCHE_LR, TAMANO_PARCHE_LR), interpolation=INTERPOLACION_LR)

                    # k=0 (0º), k=1 (90º), k=2 (180º), k=3 (270º)
                    rotaciones = [0, 1, 2, 3] if ROTACION_AUGMENTATION else [0]
                    
                    for k in rotaciones:
                        img_hr_rot = np.rot90(parche_hr_final, k)
                        img_lr_rot = np.rot90(parche_lr_final, k)
                        
                        # Formato: ZipID_Contador_ZoomX10_Rotacion.png
                        # Ejemplo: 001_00005_z20_r1.png (Zoom 2.0, Rotación 90º)
                        nombre_archivo = f'{indice_zip:03d}_{contador_parches_zip:06d}_z{int(escala_zoom*10):02d}_r{k}.png'
                        
                        try:
                            ruta_hr = Path(CARPETA_SALIDA_PATCHES_HR) / nombre_archivo
                            Image.fromarray(img_hr_rot, 'RGB').save(ruta_hr)
                            
                            ruta_lr = Path(CARPETA_SALIDA_PATCHES_LR) / nombre_archivo
                            Image.fromarray(img_lr_rot, 'RGB').save(ruta_lr)
                            
                        except Exception as e:
                            print(f"Error guardando: {e}")

                    contador_parches_zip += 1
                    
        print(f"  > {contador_parches_zip} recortes base procesados (x{len(rotaciones)} rotaciones generadas).")
        del imagen_grande_hr
        return contador_parches_zip * len(rotaciones)
    
if __name__ == "__main__":
    
    # Eliminar parches anteriores
    print("Limpiando carpetas de parches antiguas (si existen)...")
    if os.path.exists(CARPETA_SALIDA_PATCHES_HR):
        shutil.rmtree(CARPETA_SALIDA_PATCHES_HR)
        print(f"Borrada: {CARPETA_SALIDA_PATCHES_HR}")
    if os.path.exists(CARPETA_SALIDA_PATCHES_LR):
        shutil.rmtree(CARPETA_SALIDA_PATCHES_LR)
        print(f"Borrada: {CARPETA_SALIDA_PATCHES_LR}")
    print("Limpieza completada.")
    
    # Asegurarse que las carpetas de salida existen
    os.makedirs(CARPETA_SALIDA_PATCHES_HR, exist_ok=True)
    os.makedirs(CARPETA_SALIDA_PATCHES_LR, exist_ok=True)
    
    lista_zips = sorted(list(Path(CARPETA_RAIZ_ZIP).glob('*.zip')))
    if not lista_zips:
        print(f"No se encontró ningún archivo .zip en: {CARPETA_RAIZ_ZIP}")
        exit()
        
    print(f"Se encontraron {len(lista_zips)} archivos .zip para procesar.")
    print(f"Configuración: Zoom={ESCALAS_ZOOM}, Rotación={ROTACION_AUGMENTATION}")
    
    contador_total_imagenes = 0

    for indice_zip, ruta_zip in enumerate(tqdm(lista_zips, desc="Procesando Zips", unit="archivo")):
        try:
            imgs_creadas = procesar_zip_a_parches(ruta_zip, indice_zip)
            if imgs_creadas:
                contador_total_imagenes += imgs_creadas
        except Exception as e:
            print(f"\nERROR al procesar {ruta_zip.name}\nDetalle: {e}")
            
    print(f"\n--- Proceso completado ---")
    print(f"Se han guardado un total de {contador_total_imagenes} imágenes finales (HR+LR).")