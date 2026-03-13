import warnings
warnings.filterwarnings("ignore")

import argparse
import cv2
import numpy as np
import os
import torch
# Importamos la arquitectura de la red desde los archivos de KAIR
from KAIR.models.network_swinir import SwinIR as net


# Configura y carga el modelo SwinIR en memoria
# Detecta si es un modelo tipo GAN o PSNR para ajustar la arquitectura
def definir_modelo(ruta_modelo, escala=4, es_realsr=True, dispositivo='cuda'):

    print(f"   > Cargando pesos desde: {ruta_modelo}")
    checkpoint = torch.load(ruta_modelo, map_location=lambda storage, loc: storage)

    if es_realsr or "realSR" in ruta_modelo or "GAN" in ruta_modelo:
         tipo_upsampler = 'nearest+conv'
         print("   > Configuración: RealSR (nearest+conv)")
    else:
         tipo_upsampler = 'pixelshuffle'
         print("   > Configuración: Clásico (pixelshuffle)")

    # Definimos la red con el upsampler decidido
    modelo = net(upscale=escala, in_chans=3, img_size=64, window_size=8,
                img_range=1., depths=[6, 6, 6, 6, 6, 6], embed_dim=180,
                num_heads=[6, 6, 6, 6, 6, 6], mlp_ratio=2,
                upsampler=tipo_upsampler, resi_connection='1conv')

    # Carga de pesos
    if 'params_ema' in checkpoint:
        modelo.load_state_dict(checkpoint['params_ema'], strict=True)
    elif 'params' in checkpoint:
        modelo.load_state_dict(checkpoint['params'], strict=True)
    else:
        modelo.load_state_dict(checkpoint, strict=True)

    modelo.eval()
    modelo = modelo.to(dispositivo)
    return modelo


# Realiza la super-resolución cortando la imagen en trozos (tiling) para no saturar la VRAM
# Reconstruye la imagen final promediando las zonas de solapamiento
def predecir_por_bloques(img_baja_res, modelo, escala, tamano_bloque, solapamiento=32):

    b, c, h, w = img_baja_res.size()
    tile = min(tamano_bloque, h, w)
    
    if tile <= solapamiento:
        print(f"   > Imagen muy pequeña ({h}x{w}). Ignorando Tiling.")
        with torch.no_grad():
            return modelo(img_baja_res)

    # Swin Transformer funciona con ventanas de atención de 8x8
    assert tile % 8 == 0, "El tamaño del tile debe ser múltiplo de 8"
    
    stride = tile - solapamiento
    lista_h = list(range(0, h-tile, stride)) + [h-tile]
    lista_w = list(range(0, w-tile, stride)) + [w-tile]
    
    # E es el tensor donde acumularemos la imagen Escalada (resultado)
    E = torch.zeros(b, c, h*escala, w*escala).type_as(img_baja_res)
    # W es el tensor de Pesos para contar cuántas veces se ha predicho cada píxel
    W = torch.zeros_like(E)

    print(f"   > Procesando en bloques de {tile}x{tile}...")
    
    for h_idx in lista_h:
        for w_idx in lista_w:
            in_patch = img_baja_res[..., h_idx:h_idx+tile, w_idx:w_idx+tile]
            
            with torch.no_grad():
                out_patch = modelo(in_patch)
            
            out_patch_mask = torch.ones_like(out_patch)

            E[..., h_idx*escala:(h_idx+tile)*escala, w_idx*escala:(w_idx+tile)*escala].add_(out_patch)
            W[..., h_idx*escala:(h_idx+tile)*escala, w_idx*escala:(w_idx+tile)*escala].add_(out_patch_mask)
            
    # Dividimos la suma de predicciones entre la suma de pesos para promediar las zonas solapadas
    # Esto elimina las líneas de corte visibles entre bloques.
    salida = E.div_(W)
    return salida

def main():
    parser = argparse.ArgumentParser(description="Script de Predicción SwinIR para TFG")
    parser.add_argument('--input', type=str, required=True, help='Ruta a la imagen de entrada')
    parser.add_argument('--output', type=str, required=True, help='Ruta donde guardar el resultado')
    parser.add_argument('--model', type=str, required=True, help='Ruta al archivo .pth')
    parser.add_argument('--tile', type=int, default=None, help='Tamaño del bloque')

    # --- CAMBIO 2: Nuevo argumento para desactivar RealSR si fuera necesario ---
    parser.add_argument('--no_realsr', action='store_true', help='Usar si el modelo es antiguo (pixelshuffle)')
    args = parser.parse_args()

    dispositivo = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"--- Iniciando Predicción en {dispositivo} ---")

    # Por defecto asumimos que SÍ es RealSR (tu caso), salvo que digas lo contrario
    es_realsr_activado = not args.no_realsr

    modelo = definir_modelo(args.model, escala=4, es_realsr=es_realsr_activado, dispositivo=dispositivo)

    print(f"Leyendo imagen: {args.input}")
    # OpenCV lee en formato BGR y valores 0-255. Convertimos a float32 [0, 1]
    img_lq = cv2.imread(args.input, cv2.IMREAD_COLOR).astype(np.float32) / 255.
    
    # PyTorch espera formato de canales primero (C, H, W) en lugar de (H, W, C)
    img_lq = np.transpose(img_lq, (2, 0, 1))
    img_lq = torch.from_numpy(img_lq).float().unsqueeze(0).to(dispositivo)

    # Swin Transformer divide la imagen en ventanas de 8x8. Si la imagen no es múltiplo de 8, falla.
    tamano_ventana = 8
    _, _, h_old, w_old = img_lq.size()
    h_pad = (h_old // tamano_ventana + 1) * tamano_ventana - h_old
    w_pad = (w_old // tamano_ventana + 1) * tamano_ventana - w_old
    
    img_lq = torch.cat([img_lq, torch.flip(img_lq, [2])], 2)[:, :, :h_old + h_pad, :]
    img_lq = torch.cat([img_lq, torch.flip(img_lq, [3])], 3)[:, :, :, :w_old + w_pad]

    if args.tile is None:
        print("   > Modo: Imagen completa (¡Cuidado con la memoria VRAM!)")
        with torch.no_grad():
            salida = modelo(img_lq)
    else:
        print(f"   > Modo: Tiling (Procesando por bloques de {args.tile})")
        salida = predecir_por_bloques(img_lq, modelo, escala=4, tamano_bloque=args.tile)


    # Multiplicamos por 4 porque la salida es 4 veces más grande.
    salida = salida[..., :h_old * 4, :w_old * 4]

    # Pasamos de Tensor GPU -> Numpy CPU
    salida = salida.data.squeeze().float().cpu().clamp_(0, 1).numpy()
    # Reordenamos canales para volver a formato de imagen estándar (H, W, C)
    salida = np.transpose(salida, (1, 2, 0))
    # Escalamos de [0, 1] a [0, 255] y convertimos a enteros de 8 bits
    salida = (salida * 255.0).round().astype(np.uint8)
    

    directorio_salida = os.path.dirname(args.output)
    if directorio_salida:
        os.makedirs(directorio_salida, exist_ok=True)
        
    cv2.imwrite(args.output, salida)
    print(f"¡Éxito! Imagen guardada en: {args.output}")

if __name__ == '__main__':
    main()
