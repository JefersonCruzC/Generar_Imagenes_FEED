import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import textwrap
import gspread
import json
import shutil
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

# 1. LIMPIEZA TOTAL DE IMÁGENES PREVIAS
output_dir = "docs/images"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

# 2. PRE-CARGAR RECURSOS PARA AHORRAR TIEMPO
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
try:
    res_logo = requests.get("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQfE4betnoplLem-rHmrOt2gqS7zMBYV8D3aw&s", headers=headers, timeout=10)
    LOGO_GLOBAL = Image.open(BytesIO(res_logo.content)).convert("RGBA")
    LOGO_GLOBAL.thumbnail((350, 180), Image.Resampling.LANCZOS)
except Exception as e:
    print(f"Error cargando logo: {e}")
    LOGO_GLOBAL = None

def conectar_sheets():
    info_creds = json.loads(os.environ['GOOGLE_SHEETS_JSON'])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info_creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def generar_pieza_grafica(row):
    # FILTRO DE SEGURIDAD Y VELOCIDAD
    link = str(row.get('image_link', '')).lower()
    if not (link.endswith('.jpg') or link.endswith('.jpeg')):
        return "Error: Formato no soportado (.png u otro)"

    try:
        # Descarga del producto con timeout corto para no trabar el proceso
        res_prod = requests.get(row['image_link'], headers=headers, timeout=7)
        prod_img = Image.open(BytesIO(res_prod.content)).convert("RGBA")

        # Crear Lienzo
        canvas = Image.new('RGB', (900, 900), color='white')
        draw = ImageDraw.Draw(canvas)
        
        # Estampar Logo
        if LOGO_GLOBAL:
            canvas.paste(LOGO_GLOBAL, ((900 - LOGO_GLOBAL.width) // 2, 40), LOGO_GLOBAL)
        
        # Estampar Producto
        prod_img.thumbnail((600, 450), Image.Resampling.LANCZOS)
        canvas.paste(prod_img, ((900 - prod_img.width) // 2, 200 + (450 - prod_img.height) // 2), prod_img)
        
        # Diseño de Marca (Barra Morada)
        draw.rectangle([0, 680, 900, 900], fill=(102, 0, 153))
        
        # Cargar Fuentes
        try:
            f_titulo = ImageFont.truetype(FONT_PATH, 28)
            f_num = ImageFont.truetype(FONT_PATH, 60)
            f_sim = ImageFont.truetype(FONT_PATH, 25)
            f_tachado = ImageFont.truetype(FONT_PATH, 22)
        except:
            f_titulo = f_num = f_sim = f_tachado = ImageFont.load_default()

        # Dibujar Óvalo de Precio
        draw.rounded_rectangle([540, 710, 860, 810], radius=50, fill="white")
        p_oferta = str(row.get('sale_price', '0')).replace(" PEN", "").strip()
        draw.text((570, 745), "S/ ", font=f_sim, fill="red")
        draw.text((615, 725), p_oferta, font=f_num, fill="red")

        # Dibujar Precio Regular Tachado
        p_reg = "S/ " + str(row.get('price', '0')).replace(" PEN", "")
        draw.text((640, 815), p_reg, font=f_tachado, fill="white")
        draw.line([640, 828, 760, 828], fill="white", width=2)

        # Dibujar Título (Máximo 3 líneas)
        titulo = str(row.get('title', 'Producto'))
        lines = textwrap.wrap(titulo, width=32)
        y_pos = 720
        for line in lines[:3]:
            draw.text((40, y_pos), line, font=f_titulo, fill="white")
            y_pos += 35

        # Guardar imagen optimizada
        file_name = f"{row['id']}.jpg"
        canvas.save(os.path.join(output_dir, file_name), "JPEG", quality=70, optimize=True)
        return URL_BASE_PAGES + file_name

    except Exception:
        return "Error"

# --- INICIO DEL PROCESO ---
if __name__ == "__main__":
    print("Iniciando conexión a Google Sheets...")
    hoja = conectar_sheets()

    print("Descargando Feed TXT (110k filas aprox)...")
    df = pd.read_csv(URL_FEED, sep='\t', low_memory=False).fillna("")

    # Convertir DataFrame a lista de diccionarios para el ThreadPool
    rows = df.to_dict('records')

    print(f"Procesando {len(rows)} imágenes en paralelo (20 hilos)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        resultados = list(tqdm(executor.map(generar_pieza_grafica, rows), total=len(rows)))

    df['link_imagen_generada'] = resultados

    print("Limpiando y actualizando Google Sheets...")
    # Convertir todo a string para evitar errores de JSON en la API de Google
    df_final = df.astype(str)
    lista_final = [df_final.columns.tolist()] + df_final.values.tolist()

    hoja.clear()
    
    # Subida por bloques para evitar límites de la API de Google (5000 filas por vez)
    for i in range(0, len(lista_final), 5000):
        hoja.append_rows(lista_final[i:i+5000])
        print(f"Bloque {i} a {i+5000} subido.")

    print(f"¡Proceso completado! Total filas: {len(df)}")