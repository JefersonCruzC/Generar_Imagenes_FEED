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

# Archivos locales en el repo de GitHub
LOGO_PATH = "logojuntozblanco.png" 
FONT_BOLD = "HurmeGeometricSans1 Bold.otf"
FONT_OBLIQUE = "HurmeGeometricSans1 Oblique.otf"
FONT_REGULAR = "HurmeGeometricSans1.otf"

output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Pre-cargar logo globalmente
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
    
    if row.get('SKIP_GENERATE') and os.path.exists(target_path):
        return URL_BASE_PAGES + file_name
    
    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        
        # 1. LIENZO 1080x1080 MORADO (#8D36C5)
        color_morado = (141, 54, 197)
        canvas = Image.new('RGB', (1080, 1080), color=color_morado)
        draw = ImageDraw.Draw(canvas)
        
        # 2. CONTENEDOR BLANCO REDONDEADO
        draw.rounded_rectangle([60, 60, 1020, 810], radius=80, fill="white")
        
        # 3. PESTAÑA MORADA REDUCIDA PARA EL LOGO
        altura_pestana = 140
        draw.rounded_rectangle([680, 0, 1080, altura_pestana], radius=40, fill=color_morado)
        
        # 4. LOGO JUNTOZ
        if LOGO_GLOBAL_ORIGINAL:
            logo_w, logo_h = LOGO_GLOBAL_ORIGINAL.size
            nuevo_logo_w = 320
            nuevo_logo_h = int((nuevo_logo_w / logo_w) * logo_h)
            logo_ready = LOGO_GLOBAL_ORIGINAL.resize((nuevo_logo_w, nuevo_logo_h), Image.Resampling.LANCZOS)
            logo_x = 680 + (400 - nuevo_logo_w) // 2
            logo_y = (altura_pestana - nuevo_logo_h) // 2
            canvas.paste(logo_ready, (logo_x, logo_y), logo_ready)
        
        # 5. IMAGEN PRODUCTO
        prod_img.thumbnail((680, 520), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((1080 - prod_img.width) // 2, 140 + (580 - prod_img.height) // 2), prod_img)
        
        # 6. CONFIGURACIÓN DE FUENTES
        brand_sz = 45
        title_sz = 36
        f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        f_title = ImageFont.truetype(FONT_OBLIQUE, title_sz)
        f_reg_txt = ImageFont.truetype(FONT_REGULAR, 35)
        f_sale_val = ImageFont.truetype(FONT_BOLD, 145)
        f_simbolo = ImageFont.truetype(FONT_BOLD, 75)

        # 7. TEXTO MARCA (BRAND) - Ajuste automático
        brand_txt = str(row.get('brand', '')).upper().strip()
        while draw.textlength(brand_txt, font=f_brand) > 500 and brand_sz > 25:
            brand_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        draw.text((70, 860), brand_txt, font=f_brand, fill="white")
        
        # 8. TEXTO TÍTULO - Ajuste adaptativo
        titulo = str(row.get('title', 'Producto')).strip()
        max_w = 600
        
        def fit_text(text, font, size, limit_w):
            w_wrap = 25
            lines = textwrap.wrap(text, width=w_wrap)
            while (len(lines) > 3 or any(draw.textlength(l, font=font) > limit_w for l in lines)) and size > 24:
                size -= 2
                font = ImageFont.truetype(FONT_OBLIQUE, size)
                w_wrap += 2
                lines = textwrap.wrap(text, width=w_wrap)
            return lines, font, size

        lines, f_title, title_sz = fit_text(titulo, f_title, title_sz, max_w)
        y_txt = 920
        for line in lines[:3]:
            draw.text((70, y_txt), line, font=f_title, fill="white")
            y_txt += (title_sz + 8)

        # 9. PRECIOS (Derecha)
        p_reg = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        w_reg = draw.textlength(p_reg, font=f_reg_txt)
        draw.text((1010 - w_reg, 865), p_reg, font=f_reg_txt, fill="white")
        
        p_sale = str(row.get('sale_price','0')).replace(' PEN','').strip()
        w_sale = draw.textlength(p_sale, font=f_sale_val)
        w_simb = draw.textlength("S/", font=f_simbolo)
        
        draw.text((1010 - w_sale - w_simb - 5, 915), "S/", font=f_simbolo, fill="white")
        draw.text((1010 - w_sale, 910), p_sale, font=f_sale_val, fill="white")

        canvas.save(target_path, "JPEG", quality=95)
        return URL_BASE_PAGES + file_name
    except Exception as e:
        print(f"Error procesando ID {row['id']}: {e}")
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
    print(f"Iniciando procesamiento de {len(df)} filas...")

    with ThreadPoolExecutor(max_workers=50) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df)))

    df['image_link'] = [
        f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" 
        for res, row in zip(resultados, rows_to_process)
    ]
    
    columnas_deseadas = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    df_final = df[df['image_link'] != ""][columnas_deseadas].astype(str)
    
    print(f"Subiendo {len(df_final)} filas a Google Sheets...")
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()
    
    try:
        hoja.clear()
    except:
        time.sleep(10)
        hoja.clear()

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

    print("¡Proceso completado con éxito!")