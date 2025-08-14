# parrotfy_sync.py
import os, re, json
from datetime import datetime
from typing import Dict, List, Tuple

import gspread
from google.oauth2.credentials import Credentials
from playwright.sync_api import sync_playwright

# --------------------------- Config/env ---------------------------
SPREADSHEET_ID   = os.environ["SPREADSHEET_ID"]
DATA_SHEET_NAME  = os.environ.get("DATA_SHEET_NAME", "OCR Recepciones")
PRICES_SHEET     = os.environ.get("PRICES_SHEET_NAME", "Precios")
STRICT_PRICES    = os.environ.get("STRICT_PRICES", "1") in ("1", "true", "True")

PARROTFY_URL  = os.environ["PARROTFY_URL"].rstrip("/")
PARROTFY_USER = os.environ["PARROTFY_USER"]
PARROTFY_PASS = os.environ["PARROTFY_PASS"]

# --------------------------- Google helpers -----------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]

def load_creds() -> Credentials:
    token_info = json.loads(os.environ["GOOGLE_TOKEN"])
    return Credentials.from_authorized_user_info(token_info, scopes=SCOPES)

def open_sheet():
    creds = load_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws_data   = sh.worksheet(DATA_SHEET_NAME)
    ws_prices = sh.worksheet(PRICES_SHEET)
    return sh, ws_data, ws_prices

def read_prices(ws_prices) -> Dict[str, float]:
    rows = ws_prices.get_all_values()
    if not rows:
        return {}
    header = [h.strip().lower() for h in rows[0]]
    idx_sku = header.index("sku")
    # permite "precio" o "precios"
    idx_price = header.index("precio") if "precio" in header else header.index("precios")
    price_map: Dict[str,float] = {}
    for r in rows[1:]:
        if len(r) <= max(idx_sku, idx_price): 
            continue
        sku = re.sub(r"\D", "", r[idx_sku])  # solo dígitos
        if not sku:
            continue
        try:
            price = float(str(r[idx_price]).replace(",", "."))
        except:
            continue
        price_map[sku] = price
    return price_map

def pick_rows(ws_data) -> Tuple[List[List[str]], List[int]]:
    """
    Devuelve (filas, índices_de_fila) para enviar a Parrotfy.
    Envia solo un_recibidas > 0 y no marcadas como parrotfy_enviado.
    """
    rows = ws_data.get_all_values()
    if not rows: 
        return [], []
    header = [h.strip().lower() for h in rows[0]]
    idx_sku  = header.index("sku")
    idx_unr  = header.index("un_recibidas")
    idx_flag = header.index("parrotfy_enviado") if "parrotfy_enviado" in header else None

    pending = []
    row_indexes = []
    for i, r in enumerate(rows[1:], start=2):  # gspread 1-index; header = 1
        if len(r) <= max(idx_sku, idx_unr): 
            continue
        if idx_flag is not None and len(r) > idx_flag and str(r[idx_flag]).strip():
            # ya enviado
            continue
        sku = re.sub(r"\D", "", r[idx_sku])  # solo dígitos
        if not sku:
            continue
        try:
            qty = int(float(str(r[idx_unr]).replace(",", ".")))
        except:
            qty = 0
        if qty > 0:
            pending.append([sku, qty])
            row_indexes.append(i)
    return pending, row_indexes

def mark_sent(ws_data, row_indexes: List[int]):
    if not row_indexes:
        return
    rows = ws_data.get_all_values()
    header = [h.strip().lower() for h in rows[0]]
    if "parrotfy_enviado" not in header:
        ws_data.update_cell(1, len(header)+1, "parrotfy_enviado")
        col = len(header)+1
    else:
        col = header.index("parrotfy_enviado")+1

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updates = []
    for r in row_indexes:
        updates.append({
            "range": gspread.utils.rowcol_to_a1(r, col),
            "values": [[ts]]
        })
    # fuerza RAW para no tocar formatos
    ws_data.batch_update(updates, value_input_option="RAW")

# --------------------------- Builder de texto a pegar --------------
def build_import_text(pending_rows: List[List[str]], price_map: Dict[str,float]) -> Tuple[str, List[str]]:
    """
    Bloque: 'SKU[TAB]CANTIDAD[TAB]PRECIO' por línea.
    Si falta precio y STRICT_PRICES=True: lo omite y lo reporta.
    """
    lines = []
    missing = []
    for sku, qty in pending_rows:
        price = price_map.get(sku)
        if price is None:
            if STRICT_PRICES:
                missing.append(sku)
                continue
            price = 0.0
        # si es entero, imprimir sin .0
        price_str = str(int(price)) if float(price).is_integer() else str(price)
        lines.append(f"{sku}\t{qty}\t{price_str}")
    return "\n".join(lines), missing

# --------------------------- Playwright / Parrotfy -----------------
def run_parrotfy_import(import_text: str):
    """
    /inventory_movement_groups/new:
      - Referencia: Otro
      - Bodega: KW
      - Centro de negocio: Marketing
      - Importar lista -> pegar -> IMPORTAR -> CREAR
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # 1) Login
        page.goto(f"{PARROTFY_URL}/users/sign_in", wait_until="domcontentloaded")
        page.fill('input[name="user[email]"]', PARROTFY_USER)
        page.fill('input[name="user[password]"]', PARROTFY_PASS)
        # botón de enviar (robusto por role/name)
        page.get_by_role("button", name=re.compile("iniciar|entrar|sign in", re.I)).click()
        page.wait_for_load_state("networkidle")

        # 2) Form de nuevo movimiento
        page.goto(f"{PARROTFY_URL}/inventory_movement_groups/new", wait_until="domcontentloaded")

        # Referencia = Otro
        try:
            page.get_by_label("Referencia").click()
            page.keyboard.type("Otro")
            page.keyboard.press("Enter")
        except:
            pass

        # Bodega = KW
        try:
            page.get_by_label("Bodega").click()
            page.keyboard.type("KW")
            page.keyboard.press("Enter")
        except:
            pass

        # Centro de negocio = Marketing
        try:
            page.get_by_label("Centro de negocio").click()
            page.keyboard.type("Marketing")
            page.keyboard.press("Enter")
        except:
            pass

        # 3) Abrir 'Importar lista'
        opened = False
        for sel in [
            'button[aria-label="Importar lista de movimientos"]',
            'text=Importar lista',
            '[data-tooltip="Importar lista de movimientos"]',
            'button:has-text("Importar lista")',
            'a:has-text("Importar lista")',
        ]:
            try:
                page.click(sel, timeout=1500)
                opened = True
                break
            except:
                continue
        if not opened:
            raise RuntimeError("No pude abrir el modal 'Importar lista de movimientos'")

        # 4) Pegar líneas y confirmar en el modal (asegurar textarea del diálogo)
        dlg = page.get_by_role("dialog")
        try:
            area = dlg.locator("textarea")
            area.click()
            area.fill(import_text)
        except:
            # fallback contenteditable
            ce = dlg.locator("[contenteditable=true]")
            ce.click()
            ce.type(import_text)

        dlg.get_by_role("button", name=re.compile("importar", re.I)).click()

        # 5) Crear
        page.get_by_role("button", name=re.compile("^crear$", re.I)).click()
        page.wait_for_load_state("networkidle")

        ctx.close()
        browser.close()

# --------------------------- Main ---------------------------------
def main():
    print("[SYNC] Abriendo Google Sheet...")
    sh, ws_data, ws_prices = open_sheet()

    print("[SYNC] Leyendo precios...")
    price_map = read_prices(ws_prices)

    print("[SYNC] Leyendo filas pendientes...")
    pending_rows, row_ids = pick_rows(ws_data)
    print(f"[SYNC] Filas con un_recibidas>0 y no enviadas: {len(pending_rows)}")

    if not pending_rows:
        print("[SYNC] Nada que enviar. Fin.")
        return

    import_text, missing = build_import_text(pending_rows, price_map)

    if missing:
        print(f"[SYNC] Falta precio para {len(missing)} SKU(s). STRICT={STRICT_PRICES}")
        for s in missing[:20]:
            print("   -", s)
        if STRICT_PRICES and not import_text.strip():
            print("[SYNC] No quedó ninguna fila válida. Fin.")
            return

    print("[SYNC] Abriendo Parrotfy e importando...")
    run_parrotfy_import(import_text)

    print("[SYNC] Marcando filas como enviadas...")
    mark_sent(ws_data, row_ids)

    print("✅ Listo.")

if __name__ == "__main__":
    import re
    main()


