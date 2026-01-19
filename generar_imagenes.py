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
FONT_PATH = "LiberationSans-Bold.ttf"

output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Pre-cargar recursos globales
try:
    res_logo = requests.get("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s", headers=headers, timeout=10)
    LOGO_GLOBAL = Image.open(BytesIO(res_logo.content)).convert("RGBA")
    LOGO_GLOBAL.thumbnail((350, 180), Image.Resampling.LANCZOS)
    f_titulo = ImageFont.truetype(FONT_PATH, 28)
    f_num = ImageFont.truetype(FONT_PATH, 60)
    f_sim = ImageFont.truetype(FONT_PATH, 25)
    f_tachado = ImageFont.truetype(FONT_PATH, 22)
except:
    LOGO_GLOBAL = None
    f_titulo = f_num = f_sim = f_tachado = ImageFont.load_default()

def conectar_sheets():
    # Lógica de reintento para evitar el error 503 inicial
    intentos = 0
    while intentos < 3:
        try:
            info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
            client = gspread.authorize(creds)
            return client.open_by_key(SHEET_ID).sheet1
        except Exception as e:
            intentos += 1
            print(f"Error de conexión ({e}). Reintento {intentos}/3 en 15s...")
            time.sleep(15)
    raise Exception("No se pudo conectar a Google Sheets tras 3 intentos.")

def generar_pieza_grafica(row):
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    if row.get('SKIP_GENERATE') and os.path.exists(target_path):
        return URL_BASE_PAGES + file_name
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=5)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)
        if LOGO_GLOBAL: canvas.paste(LOGO_GLOBAL, ((900 - LOGO_GLOBAL.width) // 2, 40), LOGO_GLOBAL)
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width) // 2, 200 + (450 - prod_img.height) // 2), prod_img)
        draw.rectangle([0, 680, 900, 900], fill=(102, 0, 153))
        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        p_oferta = str(row.get('sale_price', '0')).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), p_oferta, font=f_num, fill="red")
        p_reg = "S/ " + str(row.get('price', '0')).replace(" PEN", "")
        draw.text((640, 815), p_reg, font=f_tachado, fill="white")
        draw.line([640, 828, 760, 828], fill="white", width=2)
        titulo = str(row.get('title', 'Producto'))
        lines = textwrap.wrap(titulo, width=32)
        y_pos = 720
        for line in lines[:3]:
            draw.text((40, y_pos), line, font=f_titulo, fill="white")
            y_pos += 35
        canvas.save(target_path, "JPEG", quality=65)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    
    print("Obteniendo memoria de precios...")
    try:
        data_actual = hoja.get_all_records()
        cache_precios = {str(r['id']): (str(r['sale_price']), str(r['price'])) for r in data_actual}
    except:
        cache_precios = {}

    print("Descargando Feed...")
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df = df_raw[
        (df_raw['availability'].str.lower() == 'in stock') & 
        (df_raw['image_link'].notnull()) & 
        (df_raw['image_link'] != "") & 
        (~df_raw['image_link'].str.lower().str.endswith('.png'))
    ].copy()
    df['original_image_url'] = df['image_link']

    def verificar_cambio(row):
        item_id = str(row['id'])
        if item_id in cache_precios:
            old_sale_raw = cache_precios[item_id][0].split('?v=')[0]
            if str(row['sale_price']) == old_sale_raw and str(row['price']) == cache_precios[item_id][1]:
                return True
        return False

    df['SKIP_GENERATE'] = df.apply(verificar_cambio, axis=1)
    rows_to_process = df.to_dict('records')
    with ThreadPoolExecutor(max_workers=50) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df)))

    df['image_link'] = [
        f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" 
        for res, row in zip(resultados, rows_to_process)
    ]
    
    columnas_deseadas = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    df_final = df[df['image_link'] != ""][columnas_deseadas].astype(str)
    
    print(f"Subiendo {len(df_final)} filas...")
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()
    
    # Limpieza con reintento
    try:
        hoja.clear()
    except:
        time.sleep(10)
        hoja.clear()

    # Subida por bloques de 5,000
    for i in range(0, len(lista_final), 5000):
        exito = False
        while not exito:
            try:
                hoja.append_rows(lista_final[i:i+5000], value_input_option='RAW')
                print(f"Bloque {i} subido.")
                time.sleep(2)
                exito = True
            except Exception as e:
                print(f"Error en bloque {i} ({e}). Reintentando en 15s...")
                time.sleep(15)

    print("¡Proceso completado!")