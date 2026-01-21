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
# NUEVO SUFIJO EN LA URL BASE
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
    # CAMBIO DE NOMBRE: _juntoz.jpg para romper el caché definitivamente
    file_name = f"{row['id']}_juntoz.jpg"
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
        
        # 4. BRAND DINÁMICO
        brand_txt = str(row.get('brand', '')).upper().strip()
        brand_sz = 38
        f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        while draw.textlength(brand_txt, font=f_brand) > 450 and brand_sz > 24:
            brand_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        draw.text((60, 720), brand_txt, font=f_brand, fill="white")
        
        # 5. TÍTULO DINÁMICO
        titulo = str(row.get('title', 'Producto')).strip()
        t_sz = 30
        f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
        lines = textwrap.wrap(titulo, width=28)
        while (len(lines) > 3 or any(draw.textlength(l, font=f_title) > 500 for l in lines)) and t_sz > 22:
            t_sz -= 2
            f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
            lines = textwrap.wrap(titulo, width=32)
        
        y_t = 770
        for line in lines[:3]:
            draw.text((60, y_t), line, font=f_title, fill="white")
            y_t += (t_sz + 6)

        # 6. PRECIO REGULAR DINÁMICO
        p_reg_txt = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        p_reg_sz = 30
        f_reg = ImageFont.truetype(FONT_REGULAR, p_reg_sz)
        while draw.textlength(p_reg_txt, font=f_reg) > 350 and p_reg_sz > 20:
            p_reg_sz -= 2
            f_reg = ImageFont.truetype(FONT_REGULAR, p_reg_sz)
        draw.text((840 - draw.textlength(p_reg_txt, font=f_reg), 725), p_reg_txt, font=f_reg, fill="white")
        
        # 7. PRECIO VENTA DINÁMICO
        p_sale_val = str(row.get('sale_price','0')).replace(' PEN','').strip()
        s_sz, simb_sz = 120, 62
        f_s = ImageFont.truetype(FONT_BOLD, s_sz)
        f_sm = ImageFont.truetype(FONT_BOLD, simb_sz)
        
        while draw.textlength(p_sale_val, font=f_s) > 350 and s_sz > 80:
            s_sz -= 5
            simb_sz -= 3
            f_s, f_sm = ImageFont.truetype(FONT_BOLD, s_sz), ImageFont.truetype(FONT_BOLD, simb_sz)
            
        w_s, w_sm = draw.textlength(p_sale_val, font=f_s), draw.textlength("S/", font=f_sm)
        draw.text((840 - w_s - w_sm - 8, 765 + (62-simb_sz)//2), "S/", font=f_sm, fill="white")
        draw.text((840 - w_s, 760 + (120-s_sz)//2), p_sale_val, font=f_s, fill="white")

        canvas.save(target_path, "JPEG", quality=90)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    
    # PRUEBA: 100 productos
    df_test = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].head(100).copy()
    df_test['original_image_url'] = df_test['image_link']
    
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    print(f"Generando prueba con sufijo _juntoz para {len(df_test)} productos...")
    rows_to_process = df_test.to_dict('records')
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df_test)))

    df_test['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows_to_process)]
    df_subir = df_test[df_test['image_link'] != ""][encabezados].astype(str)
    
    hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
    print("¡Prueba de cambio de nombre completada!")