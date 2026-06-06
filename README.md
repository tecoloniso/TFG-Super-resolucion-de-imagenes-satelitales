# Diseño y prueba de concepto de un sistema de super-resolución satelital mediante Swin Transformers y Redes Adversarias
  
Las imágenes satelitales de alta resolución son herramientas necesarias en áreas como el
urbanismo o la agricultura. Pero su obtención tiene un coste muy elevado. A su vez, las
fotografías de libre acceso suelen tener una resolución demasiado baja para ciertos usos.
Este Trabajo Fin de Grado analiza cómo la Inteligencia Artificial puede resolver este
problema mediante técnicas de "superresolución". Estas son capaces de mejorar
digitalmente la nitidez y el detalle de las capturas.  
El proyecto es una prueba de concepto para validar si la arquitectura Swin Transformer
puede reducir la brecha entre la calidad pública y comercial. Se ha desarrollado un sistema
completo que automatiza la obtención de datos de Sentinel-2 y el procesamiento de los
mismos.  
Se experimenta y se comparan dos enfoques de entrenamiento con dos propósitos
distintos: uno centrado en la exactitud matemática de la imagen (PSNR), y otro en la
construcción visual realista (GAN). Los resultados cualitativos demuestran que el modelo
orientado a PSNR suaviza las texturas intentando optimizar el error píxel a píxel, mientras
que el modelo GAN produce paisajes naturales realistas pero carece de fidelidad a la
realidad.  
Finalmente, la prueba de concepto valida el uso de los Transformers para democratizar la
observación espacial a nivel visual, pero concluye que al ser un problema matemáticamente
mal planteado, su aplicación analítica o científica está limitada por la aparición de
alucinaciones y la dificultad de garantizar una fidelidad cercana a la realidad.  

<img width="5662" height="8400" alt="mosaico_global1" src="https://github.com/user-attachments/assets/d479ff73-551f-4838-91ae-78aeb9c90b26" />
<img width="5239" height="8400" alt="mosaico_global2" src="https://github.com/user-attachments/assets/4e5b7644-64dc-4c80-8cc9-ca4b9884705e" />


para duplicar el entorno conda en una maquina linux:   
conda env create -f tfg_swinir_linux.yml
