import base64, hashlib, io, os, re, json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from PIL import Image
import pytesseract, cv2

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import gspread

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]

GMAIL_QUERY = os.environ["GMAIL_QUERY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

def load_creds():
    token_info = json.loads(os.environ["GOOGLE_TOKEN"])
    return Credentials.from_authorized_user_info(token_info, scopes=SCOPES)

def gmail_service(creds):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def sheets_client(creds):
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("OCR Recepciones")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="OCR Recepciones", rows=1000, cols=10)
        ws.append_row(["fecha_correo","sku","un_recibidas","message_id","img_hash","origen"])
    return ws

def preprocess(pil_img: Image.Image) -> Image.Image:
    img = np.array(pil_img.convert("L"))
    img = cv2.resize(img, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    img = cv2.medianBlur(img, 3)
    _, img = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(img)

def ocr_rows(pil_img):
    df = pytesseract.image_to_data(pil_img, lang="eng", output_type=pytesseract.Output.DATAFRAME)
    df = df.dropna(subset=["text"])
    if df.empty: return []
    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"] != ""]
    rows = []
    for (b, p, l), g in df.groupby(["block_num","par_num","line_num"]):
        g = g.sort_values("left")
        text = " ".join(g["text"].tolist())
        rows.append((g, text))
    return rows

def parse_table(pil_img):
    """Heurística: SKU = 10–16 dígitos; UN RECIBIDAS = último entero de la línea."""
    out = []
    for g, text in ocr_rows(pil_img):
        m_sku = re.search(r"\b(\d{10,16})\b", text.replace(" ", ""))
        if not m_sku:
            continue
        sku = m_sku.group(1)
        nums = re.findall(r"\b\d+\b", text)
        if not nums:
            continue
        un_recibidas = int(nums[-1])
        out.append({"sku": sku, "un_recibidas": un_recibidas})
    return out

def hash_image(pil_img):
    buf = io.BytesIO()
    pil_img.save(buf, format="PNG")
    return hashlib.sha256(buf.getvalue()).hexdigest()

def get_images_from_message(svc, user_id, msg):
    out = []
    payload = msg.get("payload", {})

    def dig(part):
        mime = part.get("mimeType","")
        filename = part.get("filename","")
        body = part.get("body",{})

        if (filename and mime.startswith("image/")) or (mime.startswith("image/") and body.get("data")):
            if body.get("attachmentId"):
                att = svc.users().messages().attachments().get(
                    userId=user_id, messageId=msg["id"], id=body["attachmentId"]
                ).execute()
                data = base64.urlsafe_b64decode(att["data"])
            else:
                data = base64.urlsafe_b64decode(body["data"])
            try:
                pil = Image.open(io.BytesIO(data)).convert("RGB")
                origin = "attachment" if filename else "inline"
                out.append((origin, pil))
            except Exception:
                pass

        for p in part.get("parts",[]):
            dig(p)

    dig(payload)
    return out

def fetch_messages(svc, user_id="me"):
    res = svc.users().messages().list(userId=user_id, q=GMAIL_QUERY, maxResults=20).execute()
    return res.get("messages", [])

def read_existing(ws):
    values = ws.get_all_values()
    if not values: return set()
    header = values[0]
    idx = {h:i for i,h in enumerate(header)}
    existing = set()
    for r in values[1:]:
        if len(r) < len(header): continue
        existing.add((r[idx["message_id"]], r[idx["sku"]], r[idx["un_recibidas"]]))
    return existing

def parse_gmail_date(date_str):
    try:
        ts = pd.to_datetime(date_str, utc=True, errors="coerce")
        if pd.isna(ts): raise ValueError
        return ts.tz_convert("America/Santiago").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now(tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

def main():
    creds = load_creds()
    svc = gmail_service(creds)
    ws = sheets_client(creds)

    existing = read_existing(ws)
    msgs = fetch_messages(svc)
    if not msgs:
        print("No hay correos que coincidan con la query.")
        return

    rows = []
    for m in msgs:
        msg = svc.users().messages().get(userId="me", id=m["id"]).execute()
        headers = {h["name"].lower(): h["value"] for h in msg["payload"].get("headers", [])}
        fecha = parse_gmail_date(headers.get("date"))
        message_id = msg.get("id","")

        images = get_images_from_message(svc, "me", msg)
        for origin, pil in images:
            pre = preprocess(pil)
            img_hash = hash_image(pre)
            for it in parse_table(pre):
                key = (message_id, str(it["sku"]), str(it["un_recibidas"]))
                if key in existing: 
                    continue
                rows.append([fecha, str(it["sku"]), str(it["un_recibidas"]), message_id, img_hash, origin])

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"✅ Agregadas {len(rows)} filas nuevas.")
    else:
        print("No hay filas nuevas para agregar.")

if __name__ == "__main__":
    main()
