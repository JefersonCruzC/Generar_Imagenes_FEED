import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap
import gspread
import json
import shutil
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURACIÓN ---
URL_FEED = "https://juntozstgsrvproduction.blob.core.windows.net/juntoz-feeds/google_juntoz_feed.txt"
SHEET_ID = "1KcN52kIvCOfmIMIbvIKEXHNALZ-tRpAqxo6Hg-JmbTw"
USUARIO_GITHUB = "JefersonCruzC" 
REPO_NOMBRE = "Generar_Imagenes_FEED" 
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"

# 1. LIMPIEZA DE ALMACENAMIENTO
output_dir = "docs/images"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir) # Borra todo lo del día anterior
os.makedirs(output_dir, exist_ok=True)

def conectar_sheets():
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res_prod = requests.get(row['image_link'], headers=headers, timeout=5)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        res_logo = requests.get("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s", headers=headers, timeout=5)
        logo_img = Image.open(BytesIO(res_logo.content)).convert("RGBA")

        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)
        # --- (Mismo código de diseño que ya tienes) ---
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

        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        p_oferta = str(row['sale_price']).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), p_oferta, font=f_num, fill="red")

        p_reg = "S/ " + str(row['price']).replace(" PEN", "")
        draw.text((640, 815), p_reg, font=f_tachado, fill="white")
        draw.line([640, 828, 760, 828], fill="white", width=2)

        lines = textwrap.wrap(str(row['title']), width=32)
        y = 725
        for line in lines[:3]:
            draw.text((40, y), line, font=f_titulo, fill="white")
            y += 35

        file_name = f"{row['id']}.jpg"
        # OPTIMIZACIÓN: Bajamos calidad a 75% para ahorrar espacio en 100k filas
        canvas.save(os.path.join(output_dir, file_name), "JPEG", quality=75, optimize=True)
        return URL_BASE_PAGES + file_name
    except:
        return "Error"

# --- PROCESO PRINCIPAL ---
hoja = conectar_sheets()
df = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")

# Para los 100k, GitHub Actions tardará unas 3-4 horas. 
# Lo ideal es procesar bloques de 5000 por día o según lo que necesites.
df_chunk = df.head(10000).copy() # Empieza con 5000 para no fallar por tiempo

df_chunk['link_imagen_generada'] = df_chunk.apply(generar_pieza_grafica, axis=1)

# Sobre-escribir Google Sheets
final_data = [df_chunk.columns.tolist()] + df_chunk.values.tolist()
hoja.clear()
hoja.update('A1', final_data)