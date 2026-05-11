import argparse
import cv2
import matplotlib.pyplot as plt
import numpy as np
import os
import subprocess
import sys
import random

def ejecutar_comando(comando):
    print(f"   > Ejecutando: {' '.join(comando)}")
    resultado = subprocess.run(comando, capture_output=True, text=True)
    if resultado.returncode != 0:
        print(f"\n[ERROR] El subproceso falló:\n{resultado.stderr}")
        sys.exit(1)

def degradar_realista(img_hr, escala):
    sigma = random.uniform(0.5, 1.5)
    img_blur = cv2.GaussianBlur(img_hr, (5, 5), sigma)
    h, w, _ = img_blur.shape
    new_h = h // escala
    new_w = w // escala
    img_lr = cv2.resize(img_blur, (new_w, new_h), interpolation=cv2.INTER_CUBIC)
    noise_level = random.uniform(2, 10)
    noise = np.random.normal(0, noise_level, img_lr.shape).astype(np.float32)
    img_lr_noisy = img_lr.astype(np.float32) + noise
    print(f"   > [Generación LR] Sigma Blur: {sigma:.2f} | Nivel Ruido: {noise_level:.2f}")
    return np.clip(img_lr_noisy, 0, 255).astype(np.uint8)

def aplicar_modcrop(img, escala):
    h, w = img.shape[:2]
    h_nuevo = h - (h % escala)
    w_nuevo = w - (w % escala)
    if h_nuevo != h or w_nuevo != w:
        print(f"   > [Modcrop] Imagen ajustada a ({h_nuevo}x{w_nuevo}) para la escala x{escala}.")
    return img[:h_nuevo, :w_nuevo, ...]

def calcular_mapa_error(img_hr_rgb, img_pred_rgb):
    hr_f = img_hr_rgb.astype(np.float32)
    pred_f = img_pred_rgb.astype(np.float32)
    diff = np.abs(hr_f - pred_f)
    diff_gray = np.mean(diff, axis=2)
    max_error_visual = 50.0
    diff_norm = np.clip(diff_gray / max_error_visual, 0, 1) * 255.0
    diff_norm = diff_norm.astype(np.uint8)
    heatmap_bgr = cv2.applyColorMap(diff_norm, cv2.COLORMAP_JET)
    heatmap_rgb = cv2.cvtColor(heatmap_bgr, cv2.COLOR_BGR2RGB)
    return heatmap_rgb

def main():
    parser = argparse.ArgumentParser(description="Orquestador Dinámico de Inferencias y Mosaicos")
    parser.add_argument('--hr', type=str, required=True, help='Ruta de la imagen Original HR')
    parser.add_argument('--output', type=str, required=True, help='Ruta para guardar el mosaico final')
    parser.add_argument('--scale', type=int, default=4, help='Factor de escala')
    parser.add_argument('--models', nargs='+', required=True, help='Lista de modelos en formato "Titulo=Ruta"')
    parser.add_argument('--error_map', action='store_true', help='Generar mosaico de mapas de error')
    parser.add_argument('--tile', type=int, default=None, help='Tamaño del bloque (ej. 128) para no saturar la GPU')

    args = parser.parse_args()

    temp_dir = "temp_inferencias"
    os.makedirs(temp_dir, exist_ok=True)
    diccionario_imagenes = {}

    print("--- 1. Preparando Datos ---")
    img_hr_bgr = cv2.imread(args.hr)
    if img_hr_bgr is None:
        raise FileNotFoundError(f"No se pudo cargar la imagen HR: {args.hr}")

    img_hr_bgr = aplicar_modcrop(img_hr_bgr, args.scale)
    img_hr_rgb = cv2.cvtColor(img_hr_bgr, cv2.COLOR_BGR2RGB)
    diccionario_imagenes["1. Original (HR)"] = img_hr_rgb

    print("\n--- 2. Imagen de Baja Resolución (LR) ---")
    img_lr_bgr = degradar_realista(img_hr_bgr, args.scale)
    ruta_lr_generada = os.path.join(temp_dir, "lr_generada.png")
    cv2.imwrite(ruta_lr_generada, img_lr_bgr)

    h_hr, w_hr = img_hr_bgr.shape[:2]
    img_lr_nearest_bgr = cv2.resize(img_lr_bgr, (w_hr, h_hr), interpolation=cv2.INTER_NEAREST)
    img_lr_nearest_rgb = cv2.cvtColor(img_lr_nearest_bgr, cv2.COLOR_BGR2RGB)
    diccionario_imagenes["2. Entrada Degradada (LR)"] = img_lr_nearest_rgb

    print("\n--- 3. Generando Línea Base (Bicúbica) ---")
    ruta_bicubica = os.path.join(temp_dir, "bicubica.png")
    ejecutar_comando([
        "python", "inferencia.py",
        "--input", ruta_lr_generada, "--output", ruta_bicubica,
        "--metodo", "bicubico_up", "--scale", str(args.scale)
    ])

    img_bicubica_rgb = cv2.cvtColor(cv2.imread(ruta_bicubica), cv2.COLOR_BGR2RGB)
    titulo_bicubico = "3. Bicúbica (Baseline)"
    if args.error_map:
        diccionario_imagenes[titulo_bicubico] = calcular_mapa_error(img_hr_rgb, img_bicubica_rgb)
        titulo_bicubico += " [MAPA DE ERROR]"
    else:
        diccionario_imagenes[titulo_bicubico] = img_bicubica_rgb

    print("\n--- 4. Procesando Modelos Neuronales ---")
    for idx, modelo_str in enumerate(args.models, start=4):
        try:
            titulo_raw, ruta_pesos = modelo_str.split("=")
        except ValueError:
            raise ValueError(f"Formato incorrecto en: '{modelo_str}'. Usa 'Titulo=Ruta'.")

        ruta_out_modelo = os.path.join(temp_dir, f"modelo_{idx}.png")

        comando_swinir = [
            "python", "inferencia.py",
            "--input", ruta_lr_generada, "--output", ruta_out_modelo,
            "--metodo", "swinir", "--model", ruta_pesos, "--scale", str(args.scale)
        ]
        if args.tile is not None:
            comando_swinir.extend(["--tile", str(args.tile)])

        ejecutar_comando(comando_swinir)

        img_modelo_rgb = cv2.cvtColor(cv2.imread(ruta_out_modelo), cv2.COLOR_BGR2RGB)

        titulo_final = f"{idx}. {titulo_raw}"
        if args.error_map:
            diccionario_imagenes[titulo_final] = calcular_mapa_error(img_hr_rgb, img_modelo_rgb)
            titulo_final += " [MAPA DE ERROR]"
        else:
            diccionario_imagenes[titulo_final] = img_modelo_rgb

    print("\n--- 5. Validando ---")
    ref_shape = diccionario_imagenes["1. Original (HR)"].shape
    for titulo, img in diccionario_imagenes.items():
        if img.shape != ref_shape:
            raise ValueError(f"[ERROR DE DIMENSIÓN] '{titulo}': {img.shape[:2]} vs HR {ref_shape[:2]}.")
    print("   > Dimensiones verificadas.")

    print("\n--- 6. Mosaico ---")
    num_imagenes = len(diccionario_imagenes)
    fig, axes = plt.subplots(nrows=num_imagenes, ncols=1, figsize=(20, 4 * num_imagenes))

    if num_imagenes == 1: axes = [axes]

    for ax, (titulo, img) in zip(axes, diccionario_imagenes.items()):
        ax.imshow(img)
        # Protegemos la Original y la LR de tener el fondo rojo en el título si estamos en modo error
        bg_color = 'darkred' if args.error_map and "Original" not in titulo and "Degradada" not in titulo else 'black'
        ax.text(0.01, 0.95, titulo, transform=ax.transAxes, fontsize=16,
                color='white', fontweight='bold', va='top', ha='left',
                bbox=dict(facecolor=bg_color, alpha=0.7, edgecolor='none', pad=5))
        ax.axis('off')

    plt.subplots_adjust(wspace=0, hspace=0.02, left=0, right=1, bottom=0, top=1)
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else '.', exist_ok=True)
    plt.savefig(args.output, dpi=300, bbox_inches='tight', pad_inches=0)
    print(f"¡Éxito! Mosaico guardado en: {args.output}")

if __name__ == '__main__':
    main()
