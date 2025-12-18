import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap

# --- CONFIGURACIÓN PARA GITHUB ---
USUARIO_GITHUB = "JefersonCruzC" # Tu usuario
REPO_NOMBRE = "Generar_Imagenes_FEED" # Tu repositorio
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

LOGO_URL = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s"

def generar_pieza_grafica(row):
    try:
        # 1. Cargar recursos
        response_prod = requests.get(row['image_link'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(response_prod.content)).convert("RGBA")
        response_logo = requests.get(LOGO_URL, headers=headers, timeout=10)
        logo_img = Image.open(BytesIO(response_logo.content)).convert("RGBA")

        # 2. Lienzo Cuadrado
        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)

        # 3. Logo
        logo_img.thumbnail((350, 180), Image.Resampling.LANCZOS)
        canvas.paste(logo_img, ((900 - logo_img.width) // 2, 40), logo_img)

        # 4. Producto
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width) // 2, 200 + (450 - prod_img.height) // 2), prod_img)

        # 5. Barra Morada
        draw.rectangle([0, 680, 900, 900], fill=(102, 0, 153))

        # 6. Fuentes (Asegúrate de tener el archivo .ttf en la carpeta del script)
        font_path = "LiberationSans-Bold.ttf" 
        try:
            f_titulo = ImageFont.truetype(font_path, 28)
            f_num = ImageFont.truetype(font_path, 60)
            f_sim = ImageFont.truetype(font_path, 25)
        except:
            f_titulo = f_num = f_sim = ImageFont.load_default()

        # 7. Óvalo y Precios
        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        precio_puro = str(row['sale_price']).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), precio_puro, font=f_num, fill="red")

        # 8. Título
        lines = textwrap.wrap(str(row['title']), width=32)
        y_text = 725
        for line in lines[:3]:
            draw.text((40, y_text), line, font=f_titulo, fill="white")
            y_text += 35

        # 9. Guardar en la carpeta docs/images para GitHub Pages
        output_dir = "docs/images"
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        
        file_name = f"{row['id']}.jpg"
        file_path = os.path.join(output_dir, file_name)
        canvas.save(file_path, "JPEG", quality=95)
        
        # RETORNA EL LINK PÚBLICO
        return URL_BASE_PAGES + file_name

    except Exception as e:
        print(f"Error en ID {row['id']}: {e}")
        return "Error"

# --- PROCESO ---
archivo_excel = 'FEEDOM_REEMPLAZO.xlsx'
df = pd.read_excel(archivo_excel)

print("Generando imágenes y links...")
# Creamos la nueva columna con el link directo
df['link_imagen_generada'] = df.apply(generar_pieza_grafica, axis=1)

# Guardamos el Excel actualizado
df.to_excel("FEED_ACTUALIZADO_CON_LINKS.xlsx", index=False)
print("¡Listo! Excel exportado.")