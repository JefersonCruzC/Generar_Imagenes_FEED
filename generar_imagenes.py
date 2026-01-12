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
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --- CONFIGURACIÓN ---
URL_FEED = "https://juntozstgsrvproduction.blob.core.windows.net/juntoz-feeds/google_juntoz_feed.txt"
SHEET_ID = "1KcN52kIvCOfmIMIbvIKEXHNALZ-tRpAqxo6Hg-JmbTw"
USUARIO_GITHUB = "JefersonCruzC" 
REPO_NOMBRE = "Generar_Imagenes_FEED" 
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"
FONT_PATH = "LiberationSans-Bold.ttf"

# 1. LIMPIEZA TOTAL DE IMÁGENES PREVIAS
output_dir = "docs/images"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

# 2. PRE-CARGAR RECURSOS GLOBALES
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
try:
    res_logo = requests.get("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s", headers=headers, timeout=10)
    LOGO_GLOBAL = Image.open(BytesIO(res_logo.content)).convert("RGBA")
    LOGO_GLOBAL.thumbnail((350, 180), Image.Resampling.LANCZOS)
except Exception as e:
    print(f"Error cargando logo: {e}")
    LOGO_GLOBAL = None

# CARGAR FUENTES UNA SOLA VEZ EN MEMORIA (Ahorra mucho tiempo de CPU)
try:
    f_titulo = ImageFont.truetype(FONT_PATH, 28)
    f_num = ImageFont.truetype(FONT_PATH, 60)
    f_sim = ImageFont.truetype(FONT_PATH, 25)
    f_tachado = ImageFont.truetype(FONT_PATH, 22)
except:
    f_titulo = f_num = f_sim = f_tachado = ImageFont.load_default()

def conectar_sheets():
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    link = str(row.get('image_link', '')).lower()
    if not (link.endswith('.jpg') or link.endswith('.jpeg')):
        return "Error: Formato"

    try:
        # Descarga con timeout corto para no ralentizar el proceso global
        res_prod = requests.get(row['image_link'], headers=headers, timeout=5)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")

        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)
        
        if LOGO_GLOBAL:
            canvas.paste(LOGO_GLOBAL, ((900 - LOGO_GLOBAL.width) // 2, 40), LOGO_GLOBAL)
        
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width) // 2, 200 + (450 - prod_img.height) // 2), prod_img)
        
        draw.rectangle([0, 680, 900, 900], fill=(102, 0, 153))
        
        # Óvalo de Precio
        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        p_oferta = str(row.get('sale_price', '0')).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), p_oferta, font=f_num, fill="red")

        # Precio Regular
        p_reg = "S/ " + str(row.get('price', '0')).replace(" PEN", "")
        draw.text((640, 815), p_reg, font=f_tachado, fill="white")
        draw.line([640, 828, 760, 828], fill="white", width=2)

        # Título
        titulo = str(row.get('title', 'Producto'))
        lines = textwrap.wrap(titulo, width=32)
        y_pos = 720
        for line in lines[:3]:
            draw.text((40, y_pos), line, font=f_titulo, fill="white")
            y_pos += 35

        file_name = f"{row['id']}.jpg"
        # Calidad 65 y sin optimización pesada para procesar más rápido
        canvas.save(os.path.join(output_dir, file_name), "JPEG", quality=65)
        return URL_BASE_PAGES + file_name

    except Exception:
        return "Error"

# --- INICIO DEL PROCESO ---
if __name__ == "__main__":
    print("Iniciando conexión a Google Sheets...")
    hoja = conectar_sheets()

    print("Descargando Feed...")
    df = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    rows = df.to_dict('records')

    # AUMENTO A 50 HILOS (GitHub Actions soporta bien esta concurrencia)
    print(f"Procesando {len(rows)} imágenes con 50 hilos...")
    with ThreadPoolExecutor(max_workers=50) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows), total=len(rows)))

    df['additional_image_link'] = resultados

    print("Actualizando Google Sheets...")
    df_final = df.astype(str)
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()

    hoja.clear()
    
    # Bloques de 10,000 para mayor rapidez en la subida
    for i in range(0, len(lista_final), 10000):
        hoja.append_rows(lista_final[i:i+10000], value_input_option='RAW')
        print(f"Bloque {i} subido.")

    print(f"¡Completado! Total: {len(df)}")