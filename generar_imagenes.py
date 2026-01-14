import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap
import gspread
import json
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
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # 1. SALTO INTELIGENTE
    if row.get('SKIP_GENERATE') and os.path.exists(target_path):
        return URL_BASE_PAGES + file_name

    try:
        res_prod = requests.get(row['image_link'], headers=headers, timeout=5)
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
    
    print("Obteniendo memoria de precios desde Google Sheets...")
    try:
        data_actual = hoja.get_all_records()
        cache_precios = {str(r['id']): (str(r['sale_price']), str(r['price'])) for r in data_actual}
    except Exception as e:
        print(f"Caché no disponible: {e}")
        cache_precios = {}

    print("Descargando Feed y aplicando filtros...")
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    
    # FILTROS COMBINADOS: 
    # 1. Disponibilidad 'in stock'
    # 2. image_link NO vacío
    # 3. image_link NO termina en .png
    df = df_raw[
        (df_raw['availability'].str.lower() == 'in stock') & 
        (df_raw['image_link'] != "") & 
        (~df_raw['image_link'].str.lower().str.endswith('.png'))
    ].copy()
    
    print(f"Filas procesables: {len(df)} (Se descartaron {len(df_raw) - len(df)} por stock, vacío o PNG)")

    # Lógica de comparación
    def verificar_cambio(row):
        item_id = str(row['id'])
        if item_id in cache_precios:
            old_sale, old_reg = cache_precios[item_id]
            if str(row['sale_price']) == old_sale and str(row['price']) == old_reg:
                return True
        return False

    df['SKIP_GENERATE'] = df.apply(verificar_cambio, axis=1)
    
    num_cambios = len(df[df['SKIP_GENERATE'] == False])
    print(f"Cambios detectados: {num_cambios}. Usando {len(df) - num_cambios} imágenes de caché.")

    rows_to_process = df.to_dict('records')
    with ThreadPoolExecutor(max_workers=50) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df)))

    df['additional_image_link'] = resultados
    
    # Eliminar filas donde la generación falló (resultado vacío) para limpiar el Sheets
    df_final = df[df['additional_image_link'] != ""].drop(columns=['SKIP_GENERATE']).astype(str)
    
    print(f"Subiendo {len(df_final)} filas a Google Sheets...")
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()
    hoja.clear()
    for i in range(0, len(lista_final), 10000):
        hoja.append_rows(lista_final[i:i+10000], value_input_option='RAW')

    print(f"¡Proceso completado! Tiempo ahorrado gracias al caché y filtros.")