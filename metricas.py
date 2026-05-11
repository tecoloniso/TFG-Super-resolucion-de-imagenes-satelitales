import argparse
import cv2
import numpy as np
import matplotlib.pyplot as plt
import os
import subprocess
import sys
import time
import pandas as pd
import glob
from skimage.metrics import peak_signal_noise_ratio as psnr
from skimage.metrics import structural_similarity as ssim

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import torch
import lpips

dispositivo = torch.device('cuda')
print("Cargando modelo LPIPS (VGG)...")
loss_fn_vgg = lpips.LPIPS(net='vgg').to(dispositivo)

def ejecutar_comando(comando):
    resultado = subprocess.run(comando, capture_output=True, text=True)
    if resultado.returncode != 0:
        print(f"\n[ERROR]\n{resultado.stderr}")
        sys.exit(1)

def aplicar_modcrop(img, escala):
    h, w = img.shape[:2]
    return img[:h - (h % escala), :w - (w % escala), ...]

def calcular_sam(img_hr, img_pred):
    hr = img_hr.astype(np.float32)
    pred = img_pred.astype(np.float32)
    dot_product = np.sum(hr * pred, axis=2)
    norm_hr = np.linalg.norm(hr, axis=2)
    norm_pred = np.linalg.norm(pred, axis=2)
    denom = norm_hr * norm_pred
    denom[denom == 0] = 1e-10
    cos_theta = np.clip(dot_product / denom, -1.0, 1.0)
    theta = np.arccos(cos_theta)
    return np.mean(theta)

def calcular_lpips(img_hr, img_pred):
    hr_t = torch.tensor(img_hr).permute(2, 0, 1).unsqueeze(0).float().to(dispositivo) / 255.0 * 2 - 1
    pred_t = torch.tensor(img_pred).permute(2, 0, 1).unsqueeze(0).float().to(dispositivo) / 255.0 * 2 - 1
    with torch.no_grad():
        distancia = loss_fn_vgg(hr_t, pred_t)
    return distancia.item()

def main():
    parser = argparse.ArgumentParser(description="Evaluador Cuantitativo por Lotes (Dataset)")
    parser.add_argument('--hr_dir', type=str, required=True, help='Carpeta con imágenes HR originales')
    parser.add_argument('--lr_dir', type=str, required=True, help='Carpeta con imágenes LR de entrada')
    parser.add_argument('--output_dir', type=str, default='resultados_dataset', help='Carpeta de salida')
    parser.add_argument('--scale', type=int, default=4, help='Factor escala')
    parser.add_argument('--models', nargs='+', required=True, help='"Titulo=Ruta"')
    parser.add_argument('--tile', type=int, default=None, help='Tamaño tile')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    temp_dir = "temp_batch"
    os.makedirs(temp_dir, exist_ok=True)

    hr_files = sorted(glob.glob(os.path.join(args.hr_dir, "*.*")))
    lr_files = sorted(glob.glob(os.path.join(args.lr_dir, "*.*")))

    if len(hr_files) == 0 or len(lr_files) == 0:
        raise ValueError("Error: Las carpetas HR o LR están vacías o no existen.")
    if len(hr_files) != len(lr_files):
        print(f"Aviso: Tienes {len(hr_files)} HRs y {len(lr_files)} LRs.")

    total_imagenes = min(len(hr_files), len(lr_files))
    print(f"--- Iniciando evaluación de dataset: {total_imagenes} pares de imágenes ---")

    acumuladores = { "Bicubica": {'PSNR': 0, 'SSIM': 0, 'LPIPS': 0, 'SAM': 0, 'Tiempo': 0} }
    for m in args.models:
        titulo = m.split("=")[0]
        acumuladores[titulo] = {'PSNR': 0, 'SSIM': 0, 'LPIPS': 0, 'SAM': 0, 'Tiempo': 0}

    for i in range(total_imagenes):
        ruta_hr = hr_files[i]
        ruta_lr = lr_files[i]
        nombre_base = os.path.basename(ruta_hr)
        print(f"\n[Procesando {i+1}/{total_imagenes}] -> {nombre_base}")

        img_hr = cv2.imread(ruta_hr)
        img_hr = aplicar_modcrop(img_hr, args.scale)
        img_hr_rgb = cv2.cvtColor(img_hr, cv2.COLOR_BGR2RGB)

        # Función de inferencia y medición
        def evaluar_y_sumar(titulo, comando):
            ruta_out = os.path.join(temp_dir, f"out_{titulo.replace(' ', '_')}.png")
            cmd_final = comando + ["--input", ruta_lr, "--output", ruta_out]

            t_in = time.time()
            ejecutar_comando(cmd_final)
            t_fin = time.time()
            latencia = t_fin - t_in

            img_pred_rgb = cv2.cvtColor(cv2.imread(ruta_out), cv2.COLOR_BGR2RGB)

            # Acumular
            acumuladores[titulo]['PSNR'] += psnr(img_hr_rgb, img_pred_rgb, data_range=255)
            acumuladores[titulo]['SSIM'] += ssim(img_hr_rgb, img_pred_rgb, channel_axis=2, data_range=255)
            acumuladores[titulo]['SAM'] += calcular_sam(img_hr_rgb, img_pred_rgb)
            acumuladores[titulo]['LPIPS'] += calcular_lpips(img_hr_rgb, img_pred_rgb)
            acumuladores[titulo]['Tiempo'] += latencia

        evaluar_y_sumar("Bicubica", ["python", "inferencia.py", "--metodo", "bicubico_up", "--scale", str(args.scale)])

        # Evaluar Modelos
        for modelo_str in args.models:
            titulo, ruta_pesos = modelo_str.split("=")
            cmd = ["python", "inferencia.py", "--metodo", "swinir", "--model", ruta_pesos, "--scale", str(args.scale)]
            if args.tile is not None:
                cmd.extend(["--tile", str(args.tile)])
            evaluar_y_sumar(titulo, cmd)

    resultados_finales = {}
    print("\n--- RESULTADOS MEDIOS DEL DATASET ---")
    for titulo, sumas in acumuladores.items():
        resultados_finales[titulo] = {
            'PSNR (↑)': sumas['PSNR'] / total_imagenes,
            'SSIM (↑)': sumas['SSIM'] / total_imagenes,
            'LPIPS (↓)': sumas['LPIPS'] / total_imagenes,
            'SAM (↓)': sumas['SAM'] / total_imagenes,
            'Latencia(s) (↓)': sumas['Tiempo'] / total_imagenes
        }
        r = resultados_finales[titulo]
        print(f"{titulo} -> PSNR: {r['PSNR (↑)']:.2f} | SSIM: {r['SSIM (↑)']:.4f} | LPIPS: {r['LPIPS (↓)']:.4f} | SAM: {r['SAM (↓)']:.4f}")

    # CSV de respaldo
    df = pd.DataFrame.from_dict(resultados_finales, orient='index')
    df.to_csv(os.path.join(args.output_dir, "medias_dataset.csv"))

    labels = ['PSNR', 'SSIM', 'LPIPS', 'SAM', 'Latencia']
    num_vars = len(labels)

    # Normalización de 0 a 1 para los polígonos
    df_norm = pd.DataFrame(index=df.index)

    for col in ['PSNR (↑)', 'SSIM (↑)']:
        min_val, max_val = df[col].min(), df[col].max()
        rango = (max_val - min_val) if max_val != min_val else 1
        # Margen del 10% para que el peor no colapse en el centro exacto
        df_norm[col] = 0.1 + 0.9 * ((df[col] - min_val) / rango)

    for col in ['LPIPS (↓)', 'SAM (↓)', 'Latencia(s) (↓)']:
        min_val, max_val = df[col].min(), df[col].max()
        rango = (max_val - min_val) if max_val != min_val else 1
        df_norm[col] = 0.1 + 0.9 * ((max_val - df[col]) / rango)

    angulos = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angulos += angulos[:1]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))
    colores = ['#7f8c8d', '#2980b9', '#e67e22', '#27ae60', '#8e44ad', '#c0392b']

    desplazamientos_radiales = [0] * num_vars

    for idx, (modelo, fila_norm) in enumerate(df_norm.iterrows()):
        color = colores[idx % len(colores)]
        valores_norm = fila_norm.tolist()
        valores_norm += valores_norm[:1]

        ax.plot(angulos, valores_norm, color=color, linewidth=2.5, linestyle='solid', label=modelo)
        ax.fill(angulos, valores_norm, color=color, alpha=0.1)

        fila_cruda = df.loc[modelo].tolist()
        for i, (angulo, val_norm, val_crudo) in enumerate(zip(angulos[:-1], fila_norm[:-1], fila_cruda)):
            offset = desplazamientos_radiales[i]
            radio_texto = val_norm + 0.08 + offset

            if i == 0: txt = f"{val_crudo:.1f}"      # PSNR
            elif i == 4: txt = f"{val_crudo:.1f}s"   # Latencia
            else: txt = f"{val_crudo:.3f}"           # SSIM, LPIPS, SAM

            ax.text(angulo, radio_texto, txt, color='white', size=9,
                    ha='center', va='center', fontweight='bold',
                    bbox=dict(facecolor=color, alpha=0.85, edgecolor='none', boxstyle='round,pad=0.3'))

            desplazamientos_radiales[i] += 0.06

    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angulos[:-1])
    ax.set_xticklabels(labels, fontsize=12, fontweight='bold')
    ax.set_yticklabels([])

    plt.title('Rendimiento global por iteraciones', size=16, y=1.1, fontweight='bold')

    plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), title="Modelos Evaluados", title_fontsize='11')

    ruta_grafico = os.path.join(args.output_dir, "spider_chart_dataset.png")
    plt.savefig(ruta_grafico, dpi=300, bbox_inches='tight')
    print(f"\n¡Éxito! Gráfico generado con valores numéricos integrados en: {ruta_grafico}")

if __name__ == '__main__':
    main()
