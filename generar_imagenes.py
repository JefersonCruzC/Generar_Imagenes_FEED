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
            time.sleep(15)
    raise Exception("No se pudo conectar a Google Sheets.")

def generar_pieza_grafica(row):
    # Sufijo _jz para forzar actualización visual
    file_name = f"{row['id']}_jz.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        
        # 1. LIENZO 900x900 MORADO
        color_morado = (141, 54, 197)
        canvas = Image.new('RGB', (900, 900), color=color_morado)
        draw = ImageDraw.Draw(canvas)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        
        # 2. PESTAÑA LOGO
        altura_pestana = 115
        draw.rounded_rectangle([560, 0, 900, altura_pestana], radius=35, fill=color_morado)
        
        if LOGO_GLOBAL_ORIGINAL:
            logo_w, logo_h = LOGO_GLOBAL_ORIGINAL.size
            nuevo_logo_w = 260
            logo_ready = LOGO_GLOBAL_ORIGINAL.resize((nuevo_logo_w, int((nuevo_logo_w/logo_w)*logo_h)), Image.Resampling.LANCZOS)
            canvas.paste(logo_ready, (560 + (340 - nuevo_logo_w)//2, (altura_pestana - logo_ready.height)//2), logo_ready)
        
        # 3. IMAGEN PRODUCTO
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)
        
        # 4. FUENTES
        f_brand = ImageFont.truetype(FONT_BOLD, 38)
        f_title = ImageFont.truetype(FONT_OBLIQUE, 30)
        f_reg_txt = ImageFont.truetype(FONT_REGULAR, 30)
        f_sale_val = ImageFont.truetype(FONT_BOLD, 120)
        f_simbolo = ImageFont.truetype(FONT_BOLD, 62)

        # 5. TEXTOS
        brand_txt = str(row.get('brand', '')).upper().strip()
        draw.text((60, 720), brand_txt, font=f_brand, fill="white")
        
        titulo = str(row.get('title', 'Producto')).strip()
        lines = textwrap.wrap(titulo, width=28)
        y_txt = 770
        for line in lines[:3]:
            draw.text((60, y_txt), line, font=f_title, fill="white")
            y_txt += 36

        # 6. PRECIOS
        p_reg = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        draw.text((840 - draw.textlength(p_reg, font=f_reg_txt), 725), p_reg, font=f_reg_txt, fill="white")
        
        p_sale = str(row.get('sale_price','0')).replace(' PEN','').strip()
        w_sale = draw.textlength(p_sale, font=f_sale_val)
        draw.text((840 - w_sale - draw.textlength("S/", font=f_simbolo) - 5, 765), "S/", font=f_simbolo, fill="white")
        draw.text((840 - w_sale, 760), p_sale, font=f_sale_val, fill="white")

        canvas.save(target_path, "JPEG", quality=90)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    
    # --- FILTRO DE PRUEBA: SOLO 100 PRODUCTOS ---
    df_test = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].head(100).copy()
    df_test['original_image_url'] = df_test['image_link']
    
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    print(f"Iniciando prueba con {len(df_test)} productos...")
    rows_to_process = df_test.to_dict('records')
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df_test)))

    df_test['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows_to_process)]
    df_subir = df_test[df_test['image_link'] != ""][encabezados].astype(str)
    
    hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
    
    print("¡Prueba de 100 productos completada! GitHub Pages iniciará el despliegue.")