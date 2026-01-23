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

# --- CONFIGURACIÓN DE RUTAS Y RECURSOS ---
URL_FEED = "https://juntozstgsrvproduction.blob.core.windows.net/juntoz-feeds/google_juntoz_feed.txt"
SHEET_ID = "1KcN52kIvCOfmIMIbvIKEXHNALZ-tRpAqxo6Hg-JmbTw"
USUARIO_GITHUB = "JefersonCruzC" 
REPO_NOMBRE = "Generar_Imagenes_FEED" 
URL_BASE_PAGES = f"https://{USUARIO_GITHUB}.github.io/{REPO_NOMBRE}/images/"

LOGO_PATH = "logojuntozblanco.png" 
output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)
headers = {"User-Agent": "Mozilla/5.0"}

# Carga persistente del logo para optimizar recursos del sistema
try:
    LOGO_GLOBAL = Image.open(LOGO_PATH).convert("RGBA")
except:
    LOGO_GLOBAL = None

def conectar_sheets():
    """Conexión segura mediante Service Account para manipulación del Sheets."""
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    return gspread.authorize(creds).open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    """
    IMPLEMENTACIÓN DE DISEÑO 900x900 (Versión Colab Consolidada).
    Incluye 'Barreras Invisibles' para evitar superposición de textos y precios.
    """
    price_val = str(row['sale_price']).replace(' PEN','').replace(' ','').strip()
    # Nombre de archivo basado en ID + Precio garantiza rediseño si el precio cambia
    file_name = f"{row['id']}_{price_val}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # 1. OPTIMIZACIÓN: Si el archivo con este precio ya existe físicamente, evitamos peticiones de red
    if os.path.exists(target_path):
        return URL_BASE_PAGES + file_name

    # 2. LIMPIEZA DINÁMICA: Borra versiones antiguas (precios desfasados) del mismo ID
    for f in os.listdir(output_dir):
        if f.startswith(f"{row['id']}_"):
            try: os.remove(os.path.join(output_dir, f))
            except: pass

    try:
        # Descarga de imagen origen
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")

        # --- ARQUITECTURA VISUAL ---
        # Lienzo base (Color institucional morado)
        canvas = Image.new('RGB', (900, 900), color=(141, 54, 197))
        draw = ImageDraw.Draw(canvas)

        # Contenedor de producto (Tarjeta Blanca Redondeada)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")

        # Pestaña de Logo (Superior derecha)
        draw.rounded_rectangle([560, 0, 900, 115], radius=35, fill=(141, 54, 197))

        if LOGO_GLOBAL:
            lw, lh = LOGO_GLOBAL.size
            nlw = 260 # Ancho ajustado para lienzo 900px
            logo_r = LOGO_GLOBAL.resize((nlw, int((nlw/lw)*lh)), Image.Resampling.LANCZOS)
            canvas.paste(logo_r, (560 + (340 - nlw)//2, (115 - logo_r.height)//2), logo_r)

        # Centrado de imagen de producto
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)

        # --- TIPOGRAFÍA Y BARRERAS INVISIBLES ---
        f_brand = ImageFont.truetype("HurmeGeometricSans1 Bold.otf", 38)
        f_title = ImageFont.truetype("HurmeGeometricSans1 Oblique.otf", 30)
        f_reg = ImageFont.truetype("HurmeGeometricSans1.otf", 30)
        f_sale = ImageFont.truetype("HurmeGeometricSans1 Bold.otf", 120)
        f_simb = ImageFont.truetype("HurmeGeometricSans1 Bold.otf", 62)

        # Marca (Brand) - Alineación izquierda
        draw.text((60, 720), str(row['brand']).upper().strip(), font=f_brand, fill="white")

        # Título Adaptativo - BARRERA INVISIBLE a los 500px para no chocar con el precio
        titulo = str(row['title']).strip()
        max_w_t = 500 
        lines = textwrap.wrap(titulo, width=28)
        y_t = 770
        for line in lines[:3]: # Límite de 3 líneas de texto
            if draw.textlength(line, font=f_title) <= max_w_t:
                draw.text((60, y_t), line, font=f_title, fill="white")
                y_t += 36

        # --- BLOQUE DE PRECIOS - Alineación Derecha ---
        # Precio Regular (Referencial)
        p_reg_txt = f"Precio regular: S/{row['price']}"
        draw.text((840 - draw.textlength(p_reg_txt, font=f_reg), 725), p_reg_txt, font=f_reg, fill="white")

        # Precio de Venta (Impacto visual)
        w_s = draw.textlength(price_val, font=f_sale)
        draw.text((840 - w_s - draw.textlength("S/", font=f_simb) - 5, 765), "S/", font=f_simb, fill="white")
        draw.text((840 - w_s, 760), price_val, font=f_sale, fill="white")

        # Guardado en formato JPEG con calidad optimizada
        canvas.save(target_path, "JPEG", quality=85)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    
    # Lectura y filtrado masivo del Feed
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[
        (df_raw['availability'].str.lower() == 'in stock') & 
        (df_raw['image_link'].notnull()) &
        (~df_raw['image_link'].str.lower().str.endswith('.png'))
    ].drop_duplicates(subset=['id']).copy()
    
    df_full['original_image_url'] = df_full['image_link']

    # --- PARÁMETRO DE PRUEBA: 10 FILAS ---
    # Para validar la implementación con el analista antes de procesar las 100k
    df_test = df_full.head(10).copy()
    print(f"MODO PRUEBA: Procesando {len(df_test)} filas para validación de diseño.")

    # Sincronización limpia del Sheets
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    # Procesamiento multihilo controlado
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(executor.map(generar_pieza_grafica, df_test.to_dict('records')), total=len(df_test)))
    
    df_test['image_link'] = [f"{u}?v={int(time.time())}" if (u and "https" in u) else "" for u in results]
    df_final = df_test[df_test['image_link'] != ""][encabezados].astype(str)
    
    # Carga al entorno de Google Sheets
    hoja.append_rows(df_final.values.tolist(), value_input_option='RAW')
    print("EJECUCIÓN COMPLETADA.")