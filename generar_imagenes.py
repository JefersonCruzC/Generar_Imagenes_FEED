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

# Archivos locales en el repo
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
            print(f"Error de conexión ({e}). Reintento {intentos}/3 en 15s...")
            time.sleep(15)
    raise Exception("No se pudo conectar a Google Sheets tras 3 intentos.")

def generar_pieza_grafica(row):
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # FORZADO: Eliminamos la condición de SKIP_GENERATE para asegurar el nuevo diseño 1080x1080
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        
        # 1. LIENZO 1080x1080 MORADO
        color_morado = (141, 54, 197)
        canvas = Image.new('RGB', (1080, 1080), color=color_morado)
        draw = ImageDraw.Draw(canvas)
        
        # 2. CONTENEDOR BLANCO
        draw.rounded_rectangle([60, 60, 1020, 810], radius=80, fill="white")
        
        # 3. PESTAÑA LOGO
        altura_pestana = 140
        draw.rounded_rectangle([680, 0, 1080, altura_pestana], radius=40, fill=color_morado)
        
        # 4. LOGO
        if LOGO_GLOBAL_ORIGINAL:
            logo_w, logo_h = LOGO_GLOBAL_ORIGINAL.size
            nuevo_logo_w = 320
            logo_ready = LOGO_GLOBAL_ORIGINAL.resize((nuevo_logo_w, int((nuevo_logo_w/logo_w)*logo_h)), Image.Resampling.LANCZOS)
            canvas.paste(logo_ready, (680 + (400 - nuevo_logo_w)//2, (altura_pestana - logo_ready.height)//2), logo_ready)
        
        # 5. PRODUCTO
        prod_img.thumbnail((680, 520), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((1080 - prod_img.width)//2, 140 + (580 - prod_img.height)//2), prod_img)
        
        # 6. FUENTES
        brand_sz, title_sz = 45, 36
        f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        f_title = ImageFont.truetype(FONT_OBLIQUE, title_sz)
        f_reg_txt = ImageFont.truetype(FONT_REGULAR, 35)
        f_sale_val = ImageFont.truetype(FONT_BOLD, 145)
        f_simbolo = ImageFont.truetype(FONT_BOLD, 75)

        # 7. MARCA ADAPTATIVA
        brand_txt = str(row.get('brand', '')).upper().strip()
        while draw.textlength(brand_txt, font=f_brand) > 500 and brand_sz > 25:
            brand_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        draw.text((70, 860), brand_txt, font=f_brand, fill="white")
        
        # 8. TITULO ADAPTATIVO
        titulo = str(row.get('title', 'Producto')).strip()
        def fit_text(text, font, size, limit_w):
            w_wrap = 25
            lines = textwrap.wrap(text, width=w_wrap)
            while (len(lines) > 3 or any(draw.textlength(l, font=font) > limit_w for l in lines)) and size > 24:
                size -= 2
                font = ImageFont.truetype(FONT_OBLIQUE, size)
                w_wrap += 2
                lines = textwrap.wrap(text, width=w_wrap)
            return lines, font, size

        lines, f_title, title_sz = fit_text(titulo, f_title, title_sz, 600)
        y_txt = 920
        for line in lines[:3]:
            draw.text((70, y_txt), line, font=f_title, fill="white")
            y_txt += (title_sz + 8)

        # 9. PRECIOS
        p_reg = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        draw.text((1010 - draw.textlength(p_reg, font=f_reg_txt), 865), p_reg, font=f_reg_txt, fill="white")
        
        p_sale = str(row.get('sale_price','0')).replace(' PEN','').strip()
        w_sale = draw.textlength(p_sale, font=f_sale_val)
        draw.text((1010 - w_sale - draw.textlength("S/", font=f_simbolo) - 5, 915), "S/", font=f_simbolo, fill="white")
        draw.text((1010 - w_sale, 910), p_sale, font=f_sale_val, fill="white")

        canvas.save(target_path, "JPEG", quality=95)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    print("Descargando Feed...")
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].copy()
    df['original_image_url'] = df['image_link']
    
    rows_to_process = df.to_dict('records')
    # PROCESAMIENTO FORZADO (Sin verificar SKIP_GENERATE)
    with ThreadPoolExecutor(max_workers=40) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df)))

    df['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows_to_process)]
    df_final = df[df['image_link'] != ""][['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']].astype(str)
    
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()
    hoja.clear()
    for i in range(0, len(lista_final), 5000):
        hoja.append_rows(lista_final[i:i+5000], value_input_option='RAW')
        time.sleep(2)
    print("¡Proceso completado con nuevo diseño!")