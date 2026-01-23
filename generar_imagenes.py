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

# --- CONFIGURACIÓN DE ENTORNO ---
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
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Carga del logo para optimizar memoria
try:
    LOGO_GLOBAL = Image.open(LOGO_PATH).convert("RGBA")
except:
    LOGO_GLOBAL = None

def conectar_sheets():
    """Conexión con re-intentos para evitar caídas de red."""
    intentos = 0
    while intentos < 3:
        try:
            info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
            return gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
        except:
            intentos += 1
            time.sleep(10)
    raise Exception("Fallo crítico en conexión a Google Sheets.")

def generar_pieza_grafica(row):
    """
    MOTOR DE DISEÑO 900x900 COMPLETO.
    Incluye lógica de re-escalado de fuentes y barreras de colisión.
    """
    price_val = str(row['sale_price']).replace(' PEN','').replace(' ','').strip()
    file_name = f"{row['id']}_{price_val}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # SALTO INTELIGENTE: Si el precio es igual al del Sheets, no procesamos
    if row.get('SKIP_GENERATE'):
        return URL_BASE_PAGES + file_name

    # Limpieza de versiones viejas del mismo producto
    for f in os.listdir(output_dir):
        if f.startswith(f"{row['id']}_"):
            try: os.remove(os.path.join(output_dir, f))
            except: pass

    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")

        # 1. LIENZO Y CONTENEDORES
        canvas = Image.new('RGB', (900, 900), color=(141, 54, 197))
        draw = ImageDraw.Draw(canvas)
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        draw.rounded_rectangle([560, 0, 900, 115], radius=35, fill=(141, 54, 197))

        # 2. LOGO JUNTOZ
        if LOGO_GLOBAL:
            lw, lh = LOGO_GLOBAL.size
            nlw = 260
            logo_r = LOGO_GLOBAL.resize((nlw, int((nlw/lw)*lh)), Image.Resampling.LANCZOS)
            canvas.paste(logo_r, (560 + (340 - nlw)//2, (115 - logo_r.height)//2), logo_r)

        # 3. IMAGEN PRODUCTO
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)

        # 4. MARCA (BRAND) CON AUTO-SIZE
        brand_txt = str(row.get('brand', '')).upper().strip()
        b_sz = 38
        f_brand = ImageFont.truetype(FONT_BOLD, b_sz)
        while draw.textlength(brand_txt, font=f_brand) > 450 and b_sz > 22:
            b_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, b_sz)
        draw.text((60, 720), brand_txt, font=f_brand, fill="white")

        # 5. TÍTULO ADAPTATIVO (Barrera invisible de 500px)
        titulo = str(row.get('title', 'Producto')).strip()
        t_sz = 30
        f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
        lines = textwrap.wrap(titulo, width=28)
        # Bucle para reducir fuente si el título es muy largo y choca
        while (len(lines) > 3 or any(draw.textlength(l, font=f_title) > 500 for l in lines)) and t_sz > 20:
            t_sz -= 2
            f_title = ImageFont.truetype(FONT_OBLIQUE, t_sz)
            lines = textwrap.wrap(titulo, width=32)
        
        y_t = 770
        for line in lines[:3]:
            draw.text((60, y_t), line, font=f_title, fill="white")
            y_t += (t_sz + 6)

        # 6. PRECIOS (Alineación Derecha Protegida)
        p_reg_txt = f"Precio regular: S/{str(row.get('price','0')).replace(' PEN','')}"
        f_reg = ImageFont.truetype(FONT_REGULAR, 30)
        draw.text((840 - draw.textlength(p_reg_txt, font=f_reg), 725), p_reg_txt, font=f_reg, fill="white")

        s_sz, simb_sz = 120, 62
        f_s = ImageFont.truetype(FONT_BOLD, s_sz)
        f_sm = ImageFont.truetype(FONT_BOLD, simb_sz)
        w_s = draw.textlength(price_val, font=f_s)
        w_sm = draw.textlength("S/", font=f_sm)
        
        # S/ al costado del precio
        draw.text((840 - w_s - w_sm - 8, 765), "S/", font=f_sm, fill="white")
        draw.text((840 - w_s, 760), price_val, font=f_s, fill="white")

        canvas.save(target_path, "JPEG", quality=85, optimize=True)
        return URL_BASE_PAGES + file_name
    except:
        return ""

if __name__ == "__main__":
    hoja = conectar_sheets()
    
    # RECUPERACIÓN DE CACHÉ (Velocidad extrema)
    print("Sincronizando memoria de precios...")
    try:
        data_actual = hoja.get_all_records()
        cache_precios = {str(r['id']): str(r['sale_price']).split('?v=')[0] for r in data_actual}
    except:
        cache_precios = {}

    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[(df_raw['availability'].str.lower() == 'in stock') & 
                     (df_raw['image_link'].notnull()) &
                     (~df_raw['image_link'].str.lower().str.endswith('.png'))].drop_duplicates(subset=['id']).copy()
    
    df_full['original_image_url'] = df_full['image_link']
    
    # Marcamos qué productos se saltan (si el precio es igual al Sheets)
    df_full['SKIP_GENERATE'] = df_full.apply(lambda r: str(r['id']) in cache_precios and 
                                           cache_precios[str(r['id'])] == str(r['sale_price']), axis=1)

    # --- MODO PRUEBA: 10 FILAS ---
    df_test = df_full.head(10).copy()

    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(executor.map(generar_pieza_grafica, df_test.to_dict('records')), total=len(df_test)))
    
    df_test['image_link'] = [f"{u}?v={int(time.time())}" if (u and "https" in u) else "" for u in results]
    df_final = df_test[df_test['image_link'] != ""][encabezados].astype(str)
    
    hoja.append_rows(df_final.values.tolist(), value_input_option='RAW')
    print("PROCESO TERMINADO.")