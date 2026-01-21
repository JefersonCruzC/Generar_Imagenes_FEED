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
    file_name = f"{row['id']}_v2.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # En esta corrida de emergencia, forzamos generación total ignorando el SKIP
    # para asegurar que el espacio se gestione correctamente lote por lote
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
        
        # --- Lógica dinámica de textos ---
        brand_txt = str(row.get('brand', '')).upper().strip()
        brand_sz = 38
        f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        while draw.textlength(brand_txt, font=f_brand) > 450 and brand_sz > 24:
            brand_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        draw.text((60, 720), brand_txt, font=f_brand, fill="white")
        
        titulo = str(row.get('title', 'Producto')).strip()
        t_sz = 30
        f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
        lines = textwrap.wrap(titulo, width=28)
        while (len(lines) > 3 or any(draw.textlength(l, font=f_title) > 480 for l in lines)) and t_sz > 20:
            t_sz -= 2
            f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
            lines = textwrap.wrap(titulo, width=32)
        y_t = 770
        for line in lines[:3]:
            draw.text((60, y_t), line, font=f_title, fill="white")
            y_t += (t_sz + 6)

        p_sale_val = str(row.get('sale_price','0')).replace(' PEN','').strip()
        s_sz, simb_sz = 120, 62
        f_s = ImageFont.truetype(FONT_BOLD, s_sz)
        f_sm = ImageFont.truetype(FONT_BOLD, simb_sz)
        while draw.textlength(p_sale_val, font=f_s) > 340 and s_sz > 85:
            s_sz -= 5
            simb_sz -= 3
            f_s, f_sm = ImageFont.truetype(FONT_BOLD, s_sz), ImageFont.truetype(FONT_BOLD, simb_sz)
        w_s, w_sm = draw.textlength(p_sale_val, font=f_s), draw.textlength("S/", font=f_sm)
        draw.text((840 - w_s - w_sm - 8, 765 + (62-simb_sz)//2), "S/", font=f_sm, fill="white")
        draw.text((840 - w_s, 760 + (120-s_sz)//2), p_sale_val, font=f_s, fill="white")

        p_reg_txt = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        p_reg_sz = 30
        f_reg = ImageFont.truetype(FONT_REGULAR, p_reg_sz)
        while draw.textlength(p_reg_txt, font=f_reg) > 350 and p_reg_sz > 20:
            p_reg_sz -= 2
            f_reg = ImageFont.truetype(FONT_REGULAR, p_reg_sz)
        draw.text((840 - draw.textlength(p_reg_txt, font=f_reg), 725), p_reg_txt, font=f_reg, fill="white")

        canvas.save(target_path, "JPEG", quality=90)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].copy()
    df_full['original_image_url'] = df_full['image_link']
    
    total_filas = len(df_full)
    tamano_lote = 12000 # Reducimos lote a 12k para mayor seguridad de espacio
    
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    for inicio in range(0, total_filas, tamano_lote):
        fin = min(inicio + tamano_lote, total_filas)
        df_lote = df_full.iloc[inicio:fin].copy()
        rows_to_process = df_lote.to_dict('records')
        
        print(f"Procesando lote {inicio} a {fin}...")
        with ThreadPoolExecutor(max_workers=40) as executor:
            resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df_lote)))

        df_lote['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows_to_process)]
        df_subir = df_lote[df_lote['image_link'] != ""][encabezados].astype(str)
        
        hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
        
        # --- LIMPIEZA DE ESPACIO CLAVE ---
        print(f"Limpiando disco del lote {inicio}...")
        for f in os.listdir(output_dir):
            if f.endswith(".jpg"):
                os.remove(os.path.join(output_dir, f))
        
        time.sleep(2)
        
    print("¡Proceso finalizado con éxito optimizando espacio!")