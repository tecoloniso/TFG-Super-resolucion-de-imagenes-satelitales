import cv2
import argparse
import os
import numpy as np
#
def upscale_bicubico(ruta_entrada, ruta_salida, escala=4):
    
    if not os.path.exists(ruta_entrada):
        print(f"Error: No se encuentra la imagen {ruta_entrada}")
        return

    img = cv2.imread(ruta_entrada)
    
    if img is None:
        print(f"Error: No se pudo leer la imagen {ruta_entrada}. Verifica el formato.")
        return

    print(f"--- Procesando {os.path.basename(ruta_entrada)} (Bicúbica) ---")
    print(f"   > Dimensiones originales: {img.shape[1]}x{img.shape[0]}")

    ancho_nuevo = int(img.shape[1] * escala)
    alto_nuevo = int(img.shape[0] * escala)
    dimensiones = (ancho_nuevo, alto_nuevo)

    # Aplicar Upscaling Bicúbico (INTER_CUBIC)
    img_upscaled = cv2.resize(img, dimensiones, interpolation=cv2.INTER_CUBIC)

    print(f"   > Nuevas dimensiones (x{escala}): {ancho_nuevo}x{alto_nuevo}")
    directorio_salida = os.path.dirname(ruta_salida)
    if directorio_salida:
        os.makedirs(directorio_salida, exist_ok=True)
        
    cv2.imwrite(ruta_salida, img_upscaled)
    print(f"   > Guardado en: {ruta_salida}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upscaling Bicúbico de Imágenes (Baseline)")
    parser.add_argument("--input", required=True, help="Ruta a la imagen de entrada (LR)")
    parser.add_argument("--output", required=True, help="Ruta de salida para la imagen (HR Bicúbica)")
    parser.add_argument("--scale", type=int, default=4, help="Factor de escala (default: 4)")
    
    args = parser.parse_args()
    
    upscale_bicubico(args.input, args.output, args.scale)
