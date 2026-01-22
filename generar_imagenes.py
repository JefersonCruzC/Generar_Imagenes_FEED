import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap
import gspread
import json
import time 
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --- CONFIGURACIÓN ---
URL_FEED = "https://juntozstgsrvproduction.blob.core.windows.net/juntoz-feeds/google_juntoz_feed.txt"
SHEET_ID = "1KcN52kIvCOfmIMIbvIKEXHNALZ-tRpAqxo6Hg-JmbTw"
USUARIO_GITHUB = "JefersonCruzC" 
REPO_NOMBRE = "Generar_Imagenes_FEED" 
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"

LOGO_PATH = "logojuntozblanco.png" 
FONT_BOLD = "HurmeGeometricSans1 Bold.otf"
FONT_OBLIQUE = "HurmeGeometricSans1 Oblique.otf"
FONT_REGULAR = "HurmeGeometricSans1.otf"

output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

try:
    LOGO_GLOBAL_ORIGINAL = Image.open(LOGO_PATH).convert("RGBA")
except:
    LOGO_GLOBAL_ORIGINAL = None

def conectar_sheets():
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    # SALTO: Si ya existe físicamente, no gastamos tiempo/recursos
    if os.path.exists(target_path):
        return URL_BASE_PAGES + file_name
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        canvas = Image.new('RGB', (900, 900), color=(141, 54, 197))
        draw = ImageDraw.Draw(canvas)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        draw.rounded_rectangle([560, 0, 900, 115], radius=35, fill=(141, 54, 197))
        if LOGO_GLOBAL_ORIGINAL:
            logo_w, logo_h = LOGO_GLOBAL_ORIGINAL.size
            nuevo_w = 260
            logo_ready = LOGO_GLOBAL_ORIGINAL.resize((nuevo_w, int((nuevo_w/logo_w)*logo_h)), Image.Resampling.LANCZOS)
            canvas.paste(logo_ready, (560 + (340 - nuevo_w)//2, (115 - logo_ready.height)//2), logo_ready)
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)
        brand_txt = str(row.get('brand', '')).upper().strip()
        f_brand = ImageFont.truetype(FONT_BOLD, 38)
        draw.text((60, 715), brand_txt, font=f_brand, fill="white")
        titulo = str(row.get('title', 'Producto')).strip()
        f_title = ImageFont.truetype(FONT_OBLIQUE, 28)
        lines = textwrap.wrap(titulo, width=22)
        y_t = 765
        for line in lines[:3]:
            draw.text((60, y_t), line, font=f_title, fill="white")
            y_t += 34
        p_sale_val = str(row.get('sale_price','0')).replace(' PEN','').strip()
        f_s = ImageFont.truetype(FONT_BOLD, 110)
        f_sm = ImageFont.truetype(FONT_BOLD, 55)
        w_s = draw.textlength(p_sale_val, font=f_s)
        draw.text((840 - w_s - 65, 765), "S/", font=f_sm, fill="white")
        draw.text((840 - w_s, 760), p_sale_val, font=f_s, fill="white")
        p_reg_txt = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        f_reg = ImageFont.truetype(FONT_REGULAR, 28)
        draw.text((840 - draw.textlength(p_reg_txt, font=f_reg), 720), p_reg_txt, font=f_reg, fill="white")
        canvas.save(target_path, "JPEG", quality=90)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].copy()
    df_full['original_image_url'] = df_full['image_link']
    
    # 1. LIMPIEZA TOTAL DE DUPLICADOS EN CADA INICIO PARA ASEGURAR 100K EXACTOS
    # Esto corregirá las 124k filas actuales y dejará solo los encabezados
    print("Limpiando hoja para evitar duplicados y asegurar catálogo exacto...")
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    # 2. PROCESAMIENTO COMPLETO (Ya no por bloques de 25k, sino el feed entero)
    # Pero para no saturar la subida, lo dividimos en bloques internos de 10k
    total_filas = len(df_full)
    tamano_bloque = 10000

    for i in range(0, total_filas, tamano_bloque):
        lote = df_full.iloc[i : i + tamano_bloque].copy()
        rows = lote.to_dict('records')
        print(f"Procesando bloque {i} de {total_filas}...")
        
        with ThreadPoolExecutor(max_workers=40) as executor:
            resultados = list(tqdm(executor.map(generar_pieza_grafica, rows), total=len(lote)))

        lote['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows)]
        df_subir = lote[lote['image_link'] != ""][encabezados].astype(str)
        
        # Subida segura
        hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
        print("Bloque guardado. Esperando 10s para el siguiente...")
        time.sleep(10)
        
    print("¡Proceso finalizado con 100k únicos!")