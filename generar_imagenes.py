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
headers = {"User-Agent": "Mozilla/5.0"}

try:
    LOGO_GLOBAL = Image.open(LOGO_PATH).convert("RGBA")
except:
    LOGO_GLOBAL = None

def conectar_sheets():
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    # El nombre del archivo ahora incluye el precio para detectar cambios
    # Ejemplo: 12345_199.00.jpg
    price_clean = str(row['sale_price']).replace(' PEN','').replace(' ','')
    file_name = f"{row['id']}_{price_clean}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # 1. SI LA IMAGEN YA EXISTE CON ESE PRECIO, NO HACEMOS NADA
    if os.path.exists(target_path):
        return URL_BASE_PAGES + file_name

    # 2. SI NO EXISTE, BORRAMOS VERSIONES VIEJAS DE ESE ID (Precios antiguos)
    for f in os.listdir(output_dir):
        if f.startswith(f"{row['id']}_"):
            os.remove(os.path.join(output_dir, f))

    # 3. GENERAMOS LA NUEVA IMAGEN
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        canvas = Image.new('RGB', (900, 900), color=(141, 54, 197))
        draw = ImageDraw.Draw(canvas)
        
        # Diseño (Simplificado para el ejemplo, mantén tu lógica de redondeados)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)
        
        # Texto de Precio
        f_s = ImageFont.truetype(FONT_BOLD, 110)
        draw.text((60, 760), f"S/ {price_clean}", font=f_s, fill="white")

        canvas.save(target_path, "JPEG", quality=85)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    
    # Lectura y Limpieza (Out stock, No PNG, No Duplicados)
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[
        (df_raw['availability'].str.lower() == 'in stock') & 
        (df_raw['image_link'].notnull()) &
        (~df_raw['image_link'].str.lower().str.endswith('.png'))
    ].drop_duplicates(subset=['id']).copy()
    
    df_full['id'] = df_full['id'].astype(str)
    df_full['original_image_url'] = df_full['image_link']

    # PROCESAMIENTO POR BLOQUES DE TODO EL FEED
    TAMANO_BLOQUE = 5000
    total_productos = len(df_full)
    print(f"Iniciando procesamiento total de {total_productos} productos en bloques de {TAMANO_BLOQUE}...")

    # Limpiamos el Sheets una sola vez al inicio de la sincronización masiva
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    for inicio in range(0, total_productos, TAMANO_BLOQUE):
        fin = inicio + TAMANO_BLOQUE
        df_lote = df_full.iloc[inicio:fin].copy()
        
        print(f"Procesando bloque {inicio}-{fin}...")
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            res = list(tqdm(executor.map(generar_pieza_grafica, df_lote.to_dict('records')), total=len(df_lote)))
        
        df_lote['image_link'] = [f"{u}?v={int(time.time())}" if (u and "https" in u) else "" for u in res]
        
        # Subida al Sheets del bloque actual
        df_subir = df_lote[df_lote['image_link'] != ""][encabezados].astype(str)
        hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
        
        print(f"Bloque finalizado. Pausa de seguridad para la API...")
        time.sleep(10) # Pausa para evitar Error 502

    print("Sincronización total completada.")