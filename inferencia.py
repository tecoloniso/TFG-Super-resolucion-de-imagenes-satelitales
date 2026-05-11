import warnings
warnings.filterwarnings("ignore")

import argparse
import cv2
import numpy as np
import os
import random
import torch

# Arquitectura de la red desde los archivos de KAIR
from KAIR.models.network_swinir import SwinIR as net


def degradar_realista(img_hr, escala=4):
    # Desenfoque gaussiano aleatorio
    sigma = random.uniform(0.5, 1.5)
    img_blur = cv2.GaussianBlur(img_hr, (5, 5), sigma)

    # Reducción bicúbica
    h, w, _ = img_blur.shape
    new_h, new_w = h // escala, w // escala
    img_lr = cv2.resize(img_blur, (new_w, new_h), interpolation=cv2.INTER_CUBIC)

    # Inyección de ruido gaussiano
    noise_level = random.uniform(2, 10)
    noise = np.random.normal(0, noise_level, img_lr.shape).astype(np.float32)
    img_lr_noisy = img_lr.astype(np.float32) + noise

    print(f"   > [Degradación] Sigma: {sigma:.2f} | Ruido: {noise_level:.2f}")
    return np.clip(img_lr_noisy, 0, 255).astype(np.uint8)

def procesar_clasico(img, metodo, escala):
    if metodo == 'bicubico_up':
        h, w = int(img.shape[0] * escala), int(img.shape[1] * escala)
        return cv2.resize(img, (w, h), interpolation=cv2.INTER_CUBIC)

    elif metodo == 'bicubico_down':
        h, w = int(img.shape[0] // escala), int(img.shape[1] // escala)
        return cv2.resize(img, (w, h), interpolation=cv2.INTER_CUBIC)

    elif metodo == 'degradar':
        return degradar_realista(img, escala)

    else:
        raise ValueError(f"Método clásico desconocido: {metodo}")


def definir_modelo(ruta_modelo, escala=4, es_realsr=True, dispositivo='cuda'):
    checkpoint = torch.load(ruta_modelo, map_location=lambda storage, loc: storage)

    if es_realsr or "realSR" in ruta_modelo or "GAN" in ruta_modelo:
         tipo_upsampler = 'nearest+conv'
    else:
         tipo_upsampler = 'pixelshuffle'

    modelo = net(upscale=escala, in_chans=3, img_size=64, window_size=8,
                img_range=1., depths=[6, 6, 6, 6, 6, 6], embed_dim=180,
                num_heads=[6, 6, 6, 6, 6, 6], mlp_ratio=2,
                upsampler=tipo_upsampler, resi_connection='1conv')

    if 'params_ema' in checkpoint:
        modelo.load_state_dict(checkpoint['params_ema'], strict=True)
    elif 'params' in checkpoint:
        modelo.load_state_dict(checkpoint['params'], strict=True)
    else:
        modelo.load_state_dict(checkpoint, strict=True)

    modelo.eval()
    return modelo.to(dispositivo)

def predecir_por_bloques(img_baja_res, modelo, escala, tamano_bloque, solapamiento=32):
    b, c, h, w = img_baja_res.size()
    tile = min(tamano_bloque, h, w)

    if tile <= solapamiento:
        with torch.no_grad():
            return modelo(img_baja_res)

    assert tile % 8 == 0, "El tamaño del tile debe ser múltiplo de 8"

    stride = tile - solapamiento
    lista_h = list(range(0, h-tile, stride)) + [h-tile]
    lista_w = list(range(0, w-tile, stride)) + [w-tile]

    E = torch.zeros(b, c, h*escala, w*escala).type_as(img_baja_res)
    W = torch.zeros_like(E)

    for h_idx in lista_h:
        for w_idx in lista_w:
            in_patch = img_baja_res[..., h_idx:h_idx+tile, w_idx:w_idx+tile]
            with torch.no_grad():
                out_patch = modelo(in_patch)

            E[..., h_idx*escala:(h_idx+tile)*escala, w_idx*escala:(w_idx+tile)*escala].add_(out_patch)
            W[..., h_idx*escala:(h_idx+tile)*escala, w_idx*escala:(w_idx+tile)*escala].add_(torch.ones_like(out_patch))

    return E.div_(W)

def procesar_swinir(img_bgr, args):
    dispositivo = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    es_realsr = not args.no_realsr

    modelo = definir_modelo(args.model, args.scale, es_realsr, dispositivo)

    # Conversión OpenCV a PyTorch Tensor
    img_float = img_bgr.astype(np.float32) / 255.
    img_tensor = np.transpose(img_float, (2, 0, 1))
    img_tensor = torch.from_numpy(img_tensor).float().unsqueeze(0).to(dispositivo)

    # Padding geométrico (SwinIR exige múltiplos de 8)
    tamano_ventana = 8
    _, _, h_old, w_old = img_tensor.size()
    h_pad = (h_old // tamano_ventana + 1) * tamano_ventana - h_old
    w_pad = (w_old // tamano_ventana + 1) * tamano_ventana - w_old

    img_tensor = torch.cat([img_tensor, torch.flip(img_tensor, [2])], 2)[:, :, :h_old + h_pad, :]
    img_tensor = torch.cat([img_tensor, torch.flip(img_tensor, [3])], 3)[:, :, :, :w_old + w_pad]

    # Inferencia
    if args.tile is None:
        with torch.no_grad():
            salida = modelo(img_tensor)
    else:
        salida = predecir_por_bloques(img_tensor, modelo, args.scale, args.tile)

    # Recorte y conversión de vuelta a OpenCV
    salida = salida[..., :h_old * args.scale, :w_old * args.scale]
    salida = salida.data.squeeze().float().cpu().clamp_(0, 1).numpy()
    salida = np.transpose(salida, (1, 2, 0))
    return (salida * 255.0).round().astype(np.uint8)

def main():
    parser = argparse.ArgumentParser(description="Pipeline Unificado de Procesamiento de Imágenes TFG")
    parser.add_argument('--input', type=str, required=True, help='Ruta imagen entrada')
    parser.add_argument('--output', type=str, required=True, help='Ruta imagen salida')
    parser.add_argument('--metodo', type=str, required=True, choices=['swinir', 'bicubico_up', 'bicubico_down', 'degradar'], help='Motor a utilizar')
    parser.add_argument('--scale', type=int, default=4, help='Factor de escala')

    # Argumentos exclusivos de SwinIR
    parser.add_argument('--model', type=str, default=None, help='Ruta al archivo .pth (Requerido si metodo=swinir)')
    parser.add_argument('--tile', type=int, default=None, help='Tamaño del bloque (SwinIR)')
    parser.add_argument('--no_realsr', action='store_true', help='Desactiva RealSR (SwinIR)')
    args = parser.parse_args()

    if args.metodo == 'swinir' and args.model is None:
        raise ValueError("Error: Debes proporcionar --model si el método es 'swinir'.")

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"No se encuentra la imagen de entrada: {args.input}")

    print(f"--- Ejecutando [{args.metodo.upper()}] ---")
    img_in = cv2.imread(args.input, cv2.IMREAD_COLOR)

    if args.metodo == 'swinir':
        img_out = procesar_swinir(img_in, args)
    else:
        img_out = procesar_clasico(img_in, args.metodo, args.scale)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    cv2.imwrite(args.output, img_out)
    print(f"¡Éxito! Guardado en: {args.output}")

if __name__ == '__main__':
    main()
