import cv2
import argparse
import os
import numpy as np
import random

# Degradación compleja: Blur -> Resize -> Ruido
# Misma funcion que en descargar_y_procesar.py
def degradar_realista(img_hr, escala=4):
    # BLUR (Desenfoque)
    sigma = random.uniform(0.5, 1.5)
    img_blur = cv2.GaussianBlur(img_hr, (5, 5), sigma)

    # Resize (bicubica)
    h, w, _ = img_blur.shape
    new_h = h // escala
    new_w = w // escala
    img_lr = cv2.resize(img_blur, (new_w, new_h), interpolation=cv2.INTER_CUBIC)

    # Ruido
    noise_level = random.uniform(2, 10)
    noise = np.random.normal(0, noise_level, img_lr.shape).astype(np.float32)
    img_lr_noisy = img_lr.astype(np.float32) + noise

    # Clip para asegurar que seguimos en rango 0-255 y volver a uint8
    img_lr_final = np.clip(img_lr_noisy, 0, 255).astype(np.uint8)

    print(f"     [Info] Sigma Blur: {sigma:.2f} | Ruido: {noise_level:.2f}")

    return img_lr_final

def procesar_imagen(ruta_entrada, ruta_salida, escala=4, modo='upscale'):

    if not os.path.exists(ruta_entrada):
        print(f"Error: No se encuentra la imagen {ruta_entrada}")
        return

    img = cv2.imread(ruta_entrada)
    print(f"   > Dimensiones originales: {img.shape[1]}x{img.shape[0]}")

    if modo == 'upscale':
        ancho_nuevo = int(img.shape[1] * escala)
        alto_nuevo = int(img.shape[0] * escala)
        img_out = cv2.resize(img, (ancho_nuevo, alto_nuevo), interpolation=cv2.INTER_CUBIC)

    elif modo == 'degrade':
        img_out = degradar_realista(img, escala)

    elif modo == 'simple_down':
        # Bajada simple (solo resize, sin ruido ni blur)
        ancho_nuevo = int(img.shape[1] // escala)
        alto_nuevo = int(img.shape[0] // escala)
        img_out = cv2.resize(img, (ancho_nuevo, alto_nuevo), interpolation=cv2.INTER_CUBIC)

    print(f"   > Nuevas dimensiones: {img_out.shape[1]}x{img_out.shape[0]}")

    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    cv2.imwrite(ruta_salida, img_out)
    print(f"   > Guardado en: {ruta_salida}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herramienta multiusos: Escalar y Degradar")
    parser.add_argument("--input", required=True, help="Ruta imagen entrada")
    parser.add_argument("--output", required=True, help="Ruta imagen salida")
    parser.add_argument("--scale", type=int, default=4, help="Factor de escala (default: 4)")
    parser.add_argument("--mode", choices=['upscale', 'degrade', 'simple_down'], default='upscale',
                        help="Elige acción: 'upscale' (agrandar), 'degrade' (simular satélite), 'simple_down' (encoger simple)")

    args = parser.parse_args()

    procesar_imagen(args.input, args.output, args.scale, args.mode)
