import base64, hashlib, io, os, re, json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from PIL import Image
import pytesseract, cv2

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import gspread

# ───────────────────────────────────────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

GMAIL_QUERY = os.environ["GMAIL_QUERY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

# Idioma OCR (por defecto español+inglés). Puedes setear OCR_LANG en el workflow.
OCR_LANG = os.environ.get("OCR_LANG", "spa+eng")

print("[BOOT] main.py v6 arrancando... [MARK]=UF-8421-COL")

# ───────────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────────
def normalize_spreadsheet_id(val: str) -> str:
    """Acepta ID puro o URL completa y devuelve solo el ID."""
    val = (val or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", val)
    return m.group(1) if m else val

def clean_sku(raw) -> str:
    """
    Limpia el SKU para que no quede con apóstrofo ni caracteres no numéricos.
    - Quita un apóstrofo inicial si viene pegado (p.ej. '1780...).
    - Deja solo dígitos.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    s = s.lstrip("'")                 # quita apóstrofo inicial
    s = re.sub(r"\D", "", s)          # solo dígitos
    return s

# ───────────────────────────────────────────────────────────────────────────────
# Auth / Services
# ───────────────────────────────────────────────────────────────────────────────
def load_creds():
    token_info = json.loads(os.environ["GOOGLE_TOKEN"])
    return Credentials.from_authorized_user_info(token_info, scopes=SCOPES)

def gmail_service(creds):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def sheets_client(creds):
    gc = gspread.authorize(creds)
    sid = normalize_spreadsheet_id(SPREADSHEET_ID)
    print(f"[SHEET] Using ID: {sid}")
    sh = gc.open_by_key(sid)
    print(f"[SHEET] Abierto: {sh.title}")

    tabs = [ws.title for ws in sh.worksheets()]
    print(f"[SHEET] Pestañas disponibles: {tabs}")

    preferred = "OCR Recepciones"
    alt = "OCR Recepeciones"  # con 'p' extra
    target = preferred if preferred in tabs else (alt if alt in tabs else preferred)

    try:
        ws = sh.worksheet(target)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=preferred, rows=1000, cols=10)
        ws.append_row(["fecha_correo","sku","un_recibidas","message_id","img_hash","origen"])
        print(f"[SHEET] Creada pestaña nueva: {preferred}")
    return ws

# ───────────────────────────────────────────────────────────────────────────────
# OCR utils
# ───────────────────────────────────────────────────────────────────────────────
def preprocess(pil_img: Image.Image) -> Image.Image:
    # Subimos escala + binarización para mejorar OCR
    img = np.array(pil_img.convert("L"))
    img = cv2.resize(img, None, fx=3.0, fy=3.0, interpolation=cv2.INTER_CUBIC)
    img = cv2.medianBlur(img, 3)
    try:
        img = cv2.adaptiveThreshold(
            img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 5
        )
    except Exception:
        _, img = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(img)

def ocr_rows(pil_img):
    df = pytesseract.image_to_data(
        pil_img, lang=OCR_LANG, output_type=pytesseract.Output.DATAFRAME
    )
    df = df.dropna(subset=["text"])
    if df.empty:
        return []
    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"] != ""]
    rows = []
    for (b, p, l), g in df.groupby(["block_num","par_num","line_num"]):
        g = g.sort_values("left")
        text = " ".join(g["text"].tolist())
        rows.append((g, text))
    return rows

def parse_table(pil_img):
    """
    Extrae SKU y UN RECIBIDAS usando columnas:
      - Detecta span X de headers ENVIADAS/RECIBIDAS.
      - UN RECIBIDAS = número cuyo centro X cae bajo el span de RECIBIDAS.
      - Si hay headers pero no hay número en RECIBIDAS → asumimos 0.
      - Fallback: si no hay headers, usa el número más a la derecha (comportamiento anterior).
    """
    df = pytesseract.image_to_data(
        pil_img, lang=OCR_LANG, output_type=pytesseract.Output.DATAFRAME
    )
    df = df.dropna(subset=["text"])
    if df.empty:
        return []

    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"] != ""].copy()

    # localizar headers aproximados
    def find_col_span(pattern):
        m = df[df["text"].str.contains(pattern, case=False, regex=True)]
        if m.empty:
            return None
        r = m.sort_values(["conf", "width"], ascending=[False, False]).iloc[0]
        x1 = int(r["left"])
        x2 = x1 + int(r["width"])
        return (x1, x2)

    span_env = find_col_span(r"ENVIAD")   # ENVIADAS
    span_rec = find_col_span(r"RECIB")    # RECIBIDAS

    out = []

    for (b, p, l), g in df.groupby(["block_num","par_num","line_num"]):
        g = g.sort_values("left")
        line_txt = " ".join(g["text"].tolist())

        # SKU = 10–16 dígitos
        m_sku = re.search(r"(\d{10,16})", line_txt.replace(" ", ""))
        if not m_sku:
            continue
        sku = m_sku.group(1)

        # tokens numéricos con su centro X
        digits = g[g["text"].str.fullmatch(r"\d+")]
        if digits.empty:
            continue
        digits = digits.assign(cx=digits["left"] + digits["width"]/2).sort_values("left")

        rec = None

        # Si tenemos columna RECIBIDAS detectada, buscamos número dentro del span
        if span_rec:
            in_rec = digits[(digits["cx"] >= span_rec[0]-5) & (digits["cx"] <= span_rec[1]+5)]
            if not in_rec.empty:
                rec = int(in_rec.iloc[-1]["text"])  # el más a la derecha dentro de la col

        # Si detectamos headers pero no hay número bajo RECIBIDAS → asumimos 0
        if rec is None and (span_rec or span_env):
            rec = 0

        # Fallback si no detectamos headers: usa el número más a la derecha
        if rec is None:
            rec = int(digits.iloc[-1]["text"])

        out.append({"sku": sku, "un_recibidas": rec})

    return out

def hash_image(pil_img):
    buf = io.BytesIO()
    pil_img.save(buf, format="PNG")
    return hashlib.sha256(buf.getvalue()).hexdigest()

# ───────────────────────────────────────────────────────────────────────────────
# Gmail helpers
# ───────────────────────────────────────────────────────────────────────────────
def get_images_from_message(svc, user_id, msg):
    out = []
    payload = msg.get("payload", {})

    def dig(part, depth=0):
        mime = part.get("mimeType","")
        filename = part.get("filename","")
        body = part.get("body",{})
        headers = {h["name"].lower(): h["value"] for h in part.get("headers", [])} if part.get("headers") else {}
        cid = headers.get("content-id", "")
        print(f"[PART] depth={depth} mime={mime} filename={filename} cid={cid} "
              f"attachId={body.get('attachmentId') is not None} hasData={'data' in body}")

        if mime.startswith("image/"):
            data = None
            if body.get("attachmentId"):
                att = svc.users().messages().attachments().get(
                    userId=user_id, messageId=msg["id"], id=body["attachmentId"]
                ).execute()
                data = base64.urlsafe_b64decode(att["data"])
            elif body.get("data"):
                data = base64.urlsafe_b64decode(body["data"])

            if data:
                try:
                    pil = Image.open(io.BytesIO(data)).convert("RGB")
                    origin = "attachment" if filename else "inline"
                    out.append((origin, pil))
                    print(f"[IMG] añadido origin={origin}, size={pil.size}")
                except Exception as e:
                    print(f"[WARN] no pude abrir imagen: {e}")

        for p in part.get("parts",[]):
            dig(p, depth+1)

    dig(payload)
    print(f"[INFO] total imágenes recolectadas: {len(out)}")
    return out

def fetch_messages(svc, user_id="me"):
    res = svc.users().messages().list(userId=user_id, q=GMAIL_QUERY, maxResults=20).execute()
    return res.get("messages", [])

def read_existing(ws):
    values = ws.get_all_values()
    if not values:
        return set()
    header = values[0]
    idx = {h:i for i,h in enumerate(header)}
    existing = set()
    for r in values[1:]:
        if len(r) < len(header):
            continue
        # normalizamos el SKU para evitar duplicados por apóstrofos/formato
        sku_norm = clean_sku(r[idx["sku"]]) if "sku" in idx else ""
        un_rec = r[idx["un_recibidas"]] if "un_recibidas" in idx else ""
        existing.add((r[idx["message_id"]], sku_norm, un_rec))
    return existing

def parse_gmail_date(date_str):
    try:
        ts = pd.to_datetime(date_str, utc=True, errors="coerce")
        if pd.isna(ts):
            raise ValueError
        return ts.tz_convert("America/Santiago").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now(tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

# ───────────────────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────────────────
def main():
    print("[INFO] Entrando a main()")
    creds = load_creds()
    svc = gmail_service(creds)

    profile = svc.users().getProfile(userId="me").execute()
    print(f"[AUTH] Gmail como: {profile.get('emailAddress')}")

    ws = sheets_client(creds)

    msgs = fetch_messages(svc)
    print(f"[INFO] Mensajes que calzan con la query: {len(msgs)}")
    if not msgs:
        print("No hay correos que coincidan con la query.")
        return

    existing = read_existing(ws)
    print(f"[INFO] Filas existentes en Sheet: {len(existing)}")

    rows = []
    for m in msgs:
        msg = svc.users().messages().get(userId="me", id=m["id"]).execute()
        headers = {h["name"].lower(): h["value"] for h in msg["payload"].get("headers", [])}
        message_id = msg.get("id","")
        fecha = parse_gmail_date(headers.get("date"))

        images = get_images_from_message(svc, "me", msg)
        print(f"[INFO] Mensaje {message_id}: imágenes encontradas = {len(images)}")

        for origin, pil in images:
            pre = preprocess(pil)
            items = parse_table(pre)
            print(f"[OCR] {origin}: filas detectadas = {len(items)}")
            if not items:
                sample = pytesseract.image_to_string(pre, lang=OCR_LANG)[:400]
                print("[DEBUG] OCR sample >>>"); print(sample); print("<<< OCR sample end")

            img_hash = hash_image(pre)
            for it in items:
                sku_clean = clean_sku(it.get("sku"))
                if not sku_clean:
                    continue  # si quedó vacío, saltamos
                un_rec = str(it.get("un_recibidas", "")).strip()
                key = (message_id, sku_clean, un_rec)
                if key in existing:
                    continue
                rows.append([fecha, sku_clean, un_rec, message_id, img_hash, origin])

    print(f"[INFO] Total filas a agregar: {len(rows)}")
    for r in rows:
        print("[ROW]", r)

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"✅ Agregadas {len(rows)} filas nuevas.")
    else:
        print("No hay filas nuevas para agregar.")

if __name__ == "__main__":
    main()
