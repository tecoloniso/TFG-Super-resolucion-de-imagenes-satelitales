import rasterio
import numpy as np
from PIL import Image
from pathlib import Path
import os
import glob
import zipfile
import tempfile
from tqdm import tqdm  # Para la barra de progreso del bucle principal

# --- CONFIGURACIÓN ---
CARPETA_RAIZ_ZIP = 'datasets/Sentinel_Raw'
CARPETA_SALIDA_PNG = 'datasets/Sentinel_Raw'

LOW_PERCENTILE = 2
HIGH_PERCENTILE = 98

# Intento evitar instanciar muchas veces la imagen porque ocupa mucha memoria
def normalizar_percentiles(imagen, low_p=2, high_p=98):
    # NoData=0, lo ignoramos
    mask = imagen > 0
    if not mask.any():
        print("POSIBLE ERROR: La imagen no tiene píxeles válidos (todos son 0).")
        return np.zeros_like(imagen, dtype=np.uint8) # Imagen vacia

    p_low, p_high = np.percentile(imagen[mask], (low_p, high_p))
    
    # Eliminar valores fuera de los percentiles
    normalizado = np.clip(imagen[mask], p_low, p_high)
    # Normalizar a [0-255]
    normalizado = (((normalizado - p_low) / (p_high - p_low)) * 255).astype(np.uint8)
    salida = np.zeros_like(imagen, dtype=np.uint8)
    salida[mask] = normalizado
    return salida


def procesar_zip_a_png(ruta_zip, ruta_png_salida):

    print(f"\nProcesando: {ruta_zip.name}")
    
    # Utilizo with para asegurar que no cargo mas de una foto a la vez en memoria
    # Utilizo tempfile para no llenar el disco con archivos temporales,
    # ademas, para evitar que los archivos tengan rutas tan largas que windows no pueda manjear
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
                    return False

                for archivo in archivos_bandas:
                    zf.extract(archivo, temp_dir)
                    
        except Exception as e:
            print(f"  > ERROR al leer el .zip: {e}")
            return False

        ruta_b02 = next(Path(temp_dir).glob('**/*_B02_10m.jp2')) # Azul
        ruta_b03 = next(Path(temp_dir).glob('**/*_B03_10m.jp2')) # Verde
        ruta_b04 = next(Path(temp_dir).glob('**/*_B04_10m.jp2')) # Rojo

        print("  > Leyendo bandas con Rasterio...")
        with rasterio.open(ruta_b04) as src:
            rojo = src.read(1)
        with rasterio.open(ruta_b03) as src:
            verde = src.read(1)
        with rasterio.open(ruta_b02) as src:
            azul = src.read(1)
        
        print(f"  > Datos leídos. Dimensiones: {rojo.shape}")

        # Normalizar cada banda usando percentiles
        rojo_norm = normalizar_percentiles(rojo)
        verde_norm = normalizar_percentiles(verde)
        azul_norm = normalizar_percentiles(azul)
        
        # Apilar las bandas en un array RGB
        rgb_stack = np.stack([rojo_norm, verde_norm, azul_norm], axis=-1)
        # Liberar las imagenes que no necesitamos mas
        del rojo, verde, azul, rojo_norm, verde_norm, azul_norm 

        print(f"  > Creando imagen RGB y guardando en: {ruta_png_salida}")
        img = Image.fromarray(rgb_stack, 'RGB')
        img.save(ruta_png_salida)
        
        del rgb_stack, img
        print(f"  > PNG guardado.")
        return True
    

if __name__ == "__main__":
    # Encontrar todos los .zip en la carpeta raíz
    lista_zips = sorted(list(Path(CARPETA_RAIZ_ZIP).glob('*.zip')))
    print(f"Se encontraron {len(lista_zips)} archivos .zip para procesar.")
    
    # Iterar sobre cada .zip
    for ruta_zip in tqdm(lista_zips, desc="Procesando Zips", unit="archivo"):
        
        nombre_salida = f"{ruta_zip.stem}_RGB.png"
        ruta_png_salida = Path(CARPETA_SALIDA_PNG) / nombre_salida
        
        if ruta_png_salida.exists():
            print(f"Omitiendo {ruta_zip.name} (el PNG ya existe).")
            continue
            
        procesar_zip_a_png(ruta_zip, ruta_png_salida)
            
    print("\n--- ¡Proceso completado! ---")