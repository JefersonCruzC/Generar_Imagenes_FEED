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
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    if os.path.exists(target_path):
        return URL_BASE_PAGES + file_name
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        canvas = Image.new('RGB', (900, 900), color=(141, 54, 197))
        draw = ImageDraw.Draw(canvas)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        draw.rounded_rectangle([560, 0, 900, 115], radius=35, fill=(141, 54, 197))
        if LOGO_GLOBAL:
            logo_w, logo_h = LOGO_GLOBAL.size
            nuevo_w = 260
            logo_ready = LOGO_GLOBAL.resize((nuevo_w, int((nuevo_w/logo_w)*logo_h)), Image.Resampling.LANCZOS)
            canvas.paste(logo_ready, (560 + (340 - nuevo_w)//2, (115 - logo_ready.height)//2), logo_ready)
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)
        brand_txt = str(row.get('brand', '')).upper().strip()
        draw.text((60, 715), brand_txt, font=ImageFont.truetype(FONT_BOLD, 38), fill="white")
        titulo = textwrap.wrap(str(row.get('title', 'Producto')).strip(), width=22)
        y_t = 765
        for line in titulo[:3]:
            draw.text((60, y_t), line, font=ImageFont.truetype(FONT_OBLIQUE, 28), fill="white")
            y_t += 34
        p_sale_val = str(row.get('sale_price','0')).replace(' PEN','').strip()
        f_s = ImageFont.truetype(FONT_BOLD, 110)
        draw.text((840 - draw.textlength(p_sale_val, font=f_s), 760), p_sale_val, font=f_s, fill="white")
        canvas.save(target_path, "JPEG", quality=85)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    print("Escaneando archivos físicos...")
    archivos_reales = set([f.replace('.jpg', '') for f in os.listdir(output_dir) if f.endswith('.jpg')])
    
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].copy()
    df_full['id'] = df_full['id'].astype(str)
    df_full['original_image_url'] = df_full['image_link']
    df_full['TIENE_IMAGEN'] = df_full['id'].isin(archivos_reales)

    print("Limpiando y sincronizando Sheets...")
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    df_existentes = df_full[df_full['TIENE_IMAGEN'] == True].copy()
    df_existentes['image_link'] = df_existentes.apply(lambda r: f"{URL_BASE_PAGES}{r['id']}.jpg?v={str(r['sale_price']).replace(' ','')}", axis=1)
    
    for i in range(0, len(df_existentes), 10000):
        bloque = df_existentes.iloc[i:i+10000][encabezados].astype(str).values.tolist()
        hoja.append_rows(bloque, value_input_option='RAW')

    df_faltantes = df_full[df_full['TIENE_IMAGEN'] == False].head(10000).copy()
    if not df_faltantes.empty:
        print(f"Generando {len(df_faltantes)} nuevas...")
        with ThreadPoolExecutor(max_workers=40) as executor:
            res = list(tqdm(executor.map(generar_pieza_grafica, df_faltantes.to_dict('records')), total=len(df_faltantes)))
        
        # FILTRO DE SEGURIDAD: Solo sube si la URL contiene https (evita links rotos)
        df_faltantes['image_link'] = [f"{u}?v={str(r['sale_price']).replace(' ','')}" if (u and "https" in u) else "" for u, r in zip(res, df_faltantes.to_dict('records'))]
        df_subir = df_faltantes[df_faltantes['image_link'] != ""][encabezados].astype(str)
        if not df_subir.empty:
            hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')

    print("Proceso terminado exitosamente.")