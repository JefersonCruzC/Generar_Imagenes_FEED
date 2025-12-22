import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURACIÓN ---
URL_FEED = "https://juntozstgsrvproduction.blob.core.windows.net/juntoz-feeds/google_juntoz_feed.txt"
SHEET_ID = "1KcN52kIvCOfmIMIbvIKEXHNALZ-tRpAqxo6Hg-JmbTw"
USUARIO_GITHUB = "JefersonCruzC" 
REPO_NOMBRE = "Generar_Imagenes_FEED" 
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"

headers = {"User-Agent": "Mozilla/5.0"}
LOGO_URL = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s"

# Asegurar carpeta de imágenes
output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)

def conectar_sheets():
    # Extrae el JSON desde la variable de entorno de GitHub
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keymap(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    try:
        # Descarga rápida
        res_prod = requests.get(row['image_link'], headers=headers, timeout=5)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        res_logo = requests.get(LOGO_URL, headers=headers, timeout=5)
        logo_img = Image.open(BytesIO(res_logo.content)).convert("RGBA")

        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)

        # Diseño
        logo_img.thumbnail((350, 180), Image.Resampling.LANCZOS)
        canvas.paste(logo_img, ((900 - logo_img.width) // 2, 40), logo_img)
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width) // 2, 200 + (450 - prod_img.height) // 2), prod_img)

        draw.rectangle([0, 680, 900, 900], fill=(102, 0, 153))

        font_path = "LiberationSans-Bold.ttf"
        f_titulo = ImageFont.truetype(font_path, 28)
        f_num = ImageFont.truetype(font_path, 60)
        f_sim = ImageFont.truetype(font_path, 25)
        f_tachado = ImageFont.truetype(font_path, 22)

        # Precios
        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        precio_puro = str(row['sale_price']).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), precio_puro, font=f_num, fill="red")

        precio_reg = "S/ " + str(row['price']).replace(" PEN", "")
        draw.text((640, 815), precio_reg, font=f_tachado, fill="white")
        draw.line([640, 828, 760, 828], fill="white", width=2)

        # Título
        lines = textwrap.wrap(str(row['title']), width=32)
        y_text = 725
        for line in lines[:3]:
            draw.text((40, y_text), line, font=f_titulo, fill="white")
            y_text += 35

        file_name = f"{row['id']}.jpg"
        canvas.save(os.path.join(output_dir, file_name), "JPEG", quality=85)
        return URL_BASE_PAGES + file_name
    except:
        return "Error"

# --- PROCESO PRINCIPAL ---
print("Conectando a Google Sheets...")
hoja = conectar_sheets()

print("Leyendo Feed TXT (100k filas)...")
# Leemos el TXT con separador de tabulación \t
df = pd.read_csv(URL_FEED, sep='\t', low_memory=False)

# LIMITACIÓN: Solo procesamos los primeros 500 para evitar que GitHub Actions muera por tiempo
# Puedes subir este número a 1000 una vez veas que funciona
df_chunk = df.head(500).copy()

print(f"Generando imágenes para {len(df_chunk)} filas...")
df_chunk['link_imagen_generada'] = df_chunk.apply(generar_pieza_grafica, axis=1)

print("Enviando datos a Google Sheets...")
# Preparamos los datos: encabezados + filas
final_data = [df_chunk.columns.tolist()] + df_chunk.values.tolist()

# Limpiamos la hoja y pegamos todo
hoja.clear()
hoja.update('A1', final_data)

print("¡Todo listo! Revisa tu Google Sheets.")