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

# Archivos locales en el repositorio
LOGO_PATH = "logojuntozblanco.png" 
FONT_BOLD = "HurmeGeometricSans1 Bold.otf"
FONT_OBLIQUE = "HurmeGeometricSans1 Oblique.otf"
FONT_REGULAR = "HurmeGeometricSans1.otf"

output_dir = "docs/images"
os.makedirs(output_dir, exist_ok=True)
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Cargar logo globalmente
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
    raise Exception("No se pudo conectar a Google Sheets.")

def generar_pieza_grafica(row):
    file_name = f"{row['id']}.jpg"
    target_path = os.path.join(output_dir, file_name)
    
    # SALTO INTELIGENTE: Si ya existe y el precio no cambió, no trabajar de más
    if row.get('SKIP_GENERATE') and os.path.exists(target_path):
        return URL_BASE_PAGES + file_name

    try:
        res_prod = requests.get(row['original_image_url'], headers=headers, timeout=10)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")
        
        # 1. LIENZO 900x900 MORADO
        color_morado = (141, 54, 197)
        canvas = Image.new('RGB', (900, 900), color=color_morado)
        draw = ImageDraw.Draw(canvas)
        
        # 2. CONTENEDOR BLANCO
        draw.rounded_rectangle([50, 50, 850, 680], radius=65, fill="white")
        
        # 3. PESTAÑA LOGO
        altura_pestana = 115
        draw.rounded_rectangle([560, 0, 900, altura_pestana], radius=35, fill=color_morado)
        
        # 4. LOGO (Escalado a 900px)
        if LOGO_GLOBAL_ORIGINAL:
            logo_w, logo_h = LOGO_GLOBAL_ORIGINAL.size
            nuevo_logo_w = 260
            logo_ready = LOGO_GLOBAL_ORIGINAL.resize((nuevo_logo_w, int((nuevo_logo_w/logo_w)*logo_h)), Image.Resampling.LANCZOS)
            canvas.paste(logo_ready, (560 + (340 - nuevo_logo_w)//2, (altura_pestana - logo_ready.height)//2), logo_ready)
        
        # 5. PRODUCTO
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width)//2, 130 + (450 - prod_img.height)//2), prod_img)
        
        # 6. FUENTES ADAPTADAS
        brand_sz, title_sz = 38, 30
        f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        f_title = ImageFont.truetype(FONT_OBLIQUE, title_sz)
        f_reg_txt = ImageFont.truetype(FONT_REGULAR, 30)
        f_sale_val = ImageFont.truetype(FONT_BOLD, 120)
        f_simbolo = ImageFont.truetype(FONT_BOLD, 62)

        # 7. MARCA ADAPTATIVA
        brand_txt = str(row.get('brand', '')).upper().strip()
        while draw.textlength(brand_txt, font=f_brand) > 480 and brand_sz > 22:
            brand_sz -= 2
            f_brand = ImageFont.truetype(FONT_BOLD, brand_sz)
        draw.text((60, 720), brand_txt, font=f_brand, fill="white")
        
        # 8. TITULO ADAPTATIVO
        titulo = str(row.get('title', 'Producto')).strip()
        def fit_text(text, font, size, limit_w):
            w_wrap = 28
            lines = textwrap.wrap(text, width=w_wrap)
            while (len(lines) > 3 or any(draw.textlength(l, font=font) > limit_w for l in lines)) and size > 20:
                size -= 2
                font = ImageFont.truetype(FONT_OBLIQUE, size)
                w_wrap += 2
                lines = textwrap.wrap(text, width=w_wrap)
            return lines, font, size

        lines, f_title, title_sz = fit_text(titulo, f_title, title_sz, 500)
        y_txt = 770
        for line in lines[:3]:
            draw.text((60, y_txt), line, font=f_title, fill="white")
            y_txt += (title_sz + 6)

        # 9. PRECIOS
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
    
    print("Obteniendo memoria de precios...")
    try:
        data_actual = hoja.get_all_records()
        cache_precios = {str(r['id']): (str(r['sale_price']), str(r['price'])) for r in data_actual}
    except:
        cache_precios = {}

    print("Descargando Feed completo...")
    df_raw = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")
    df_full = df_raw[(df_raw['availability'].str.lower() == 'in stock') & (df_raw['image_link'].notnull())].copy()
    df_full['original_image_url'] = df_full['image_link']
    
    # Determinar qué productos necesitan regenerarse
    df_full['SKIP_GENERATE'] = df_full.apply(lambda r: str(r['id']) in cache_precios and 
                                   cache_precios[str(r['id'])][0].split('?v=')[0] == str(r['sale_price']) and 
                                   cache_precios[str(r['id'])][1] == str(r['price']), axis=1)

    total_filas = len(df_full)
    tamano_lote = 12000 # Procesamos en bloques para cuidar el disco
    print(f"Iniciando proceso por lotes. Total: {total_filas} productos.")

    # Limpiar hoja al inicio e insertar encabezados
    hoja.clear()
    encabezados = ['id', 'title', 'link', 'price', 'sale_price', 'availability', 'description', 'image_link', 'condition', 'brand', 'google_product_category', 'product_type']
    hoja.append_rows([encabezados], value_input_option='RAW')

    for inicio in range(0, total_filas, tamano_lote):
        fin = min(inicio + tamano_lote, total_filas)
        df_lote = df_full.iloc[inicio:fin].copy()
        rows_to_process = df_lote.to_dict('records')
        
        print(f"--- Procesando Lote {inicio} a {fin} ---")
        with ThreadPoolExecutor(max_workers=40) as executor:
            resultados = list(tqdm(executor.map(generar_pieza_grafica, rows_to_process), total=len(df_lote)))

        # Aplicar Cache Busting
        df_lote['image_link'] = [f"{res}?v={str(row['sale_price']).replace(' ', '')}" if res != "" else "" for res, row in zip(resultados, rows_to_process)]
        df_subir = df_lote[df_lote['image_link'] != ""][encabezados].astype(str)
        
        # Subir lote a Google Sheets
        hoja.append_rows(df_subir.values.tolist(), value_input_option='RAW')
        print(f"Lote {inicio}-{fin} guardado. Limpiando espacio temporal...")
        
        # Limpieza de disco local para el siguiente lote
        for f in os.listdir(output_dir):
            if f.endswith(".jpg"):
                os.remove(os.path.join(output_dir, f))
        time.sleep(2)

    print("¡Catálogo actualizado con éxito en 900x900!")