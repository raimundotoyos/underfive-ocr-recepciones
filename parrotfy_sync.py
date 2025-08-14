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
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
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
def first_visible(page, selectors, timeout=3000):
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=timeout)
            return loc
        except:
            continue
    raise RuntimeError(f"Ningún selector visible: {selectors}")

def run_parrotfy_import(import_text: str):
    """
    Login robusto + navegar al form + importar lista + crear.
    Guarda capturas para depurar si algo falla.
    """
    from pathlib import Path
    Path("pw_screens").mkdir(exist_ok=True, parents=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # 0) Ir directo a la página objetivo (si no estás logueado, te redirige al login)
        page.goto(f"{PARROTFY_URL}/inventory_movement_groups/new", wait_until="domcontentloaded")
        print("[PW] URL inicial:", page.url)
        page.screenshot(path="pw_screens/00_initial.png", full_page=True)

        # 1) ¿Hay login?
        try:
            # Intenta detectar un campo de email por múltiples variantes
            email_input = first_visible(page, [
                'input[name="user[email]"]',
                'input[type="email"]',
                'input[name*="email" i]',
                '[placeholder*="mail" i]',
                '//input[@type="email"]',
                '//label[contains(., "Email") or contains(., "Correo")]/following::input[1]',
            ], timeout=2000)

            # Aceptar cookies si estorban
            for btn in ['button:has-text("Aceptar")', 'button:has-text("Accept")', 'text=Aceptar']:
                try:
                    page.click(btn, timeout=1000)
                    break
                except:
                    pass

            # Rellenar email y password
            email_input.fill(PARROTFY_USER)

            password_input = first_visible(page, [
                'input[name="user[password]"]',
                'input[type="password"]',
                '//input[@type="password"]',
                '//label[contains(., "Contraseña") or contains(., "Password")]/following::input[1]',
            ])
            password_input.fill(PARROTFY_PASS)

            # Enviar formulario (múltiples variantes)
            first_visible(page, [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Iniciar")',
                'button:has-text("Entrar")',
                'button:has-text("Sign in")',
            ]).click()

            page.wait_for_load_state("networkidle")
            page.screenshot(path="pw_screens/01_after_login.png", full_page=True)
        except Exception as e:
            # Si no encontró login, asumimos que ya estaba logueado
            print("[PW] No se encontró formulario de login visible; continuo.")
            page.screenshot(path="pw_screens/01_no_login.png", full_page=True)

        # 2) Asegurar que estamos en la página de nuevo movimiento
        page.goto(f"{PARROTFY_URL}/inventory_movement_groups/new", wait_until="domcontentloaded")
        print("[PW] En new:", page.url)
        page.screenshot(path="pw_screens/02_new_page.png", full_page=True)

        # 3) Setear campos fijos (tolerante)
        def try_select(label, text):
            try:
                page.get_by_label(label).click()
                page.keyboard.type(text)
                page.keyboard.press("Enter")
            except:
                pass

        try_select("Referencia", "Otro")
        try_select("Bodega", "KW")
        try_select("Centro de negocio", "Marketing")

        page.screenshot(path="pw_screens/03_fields_set.png", full_page=True)

        # 4) Abrir 'Importar lista'
        opened = False
        for sel in [
            'button[aria-label="Importar lista de movimientos"]',
            'button:has-text("Importar lista")',
            'a:has-text("Importar lista")',
            '[data-tooltip="Importar lista de movimientos"]',
            'text=Importar lista de movimientos'
        ]:
            try:
                page.click(sel, timeout=1500)
                opened = True
                break
            except:
                continue
        if not opened:
            page.screenshot(path="pw_screens/04_no_modal.png", full_page=True)
            raise RuntimeError("No pude abrir el modal 'Importar lista de movimientos'")

        dlg = page.get_by_role("dialog")
        page.screenshot(path="pw_screens/05_modal_open.png", full_page=True)

        # 5) Pegar bloque en el textarea del modal
        try:
            area = dlg.locator("textarea").first
            area.click()
            area.fill(import_text)
        except:
            ce = dlg.locator("[contenteditable=true]").first
            ce.click()
            ce.type(import_text)

        dlg.get_by_role("button", name=re.compile("importar", re.I)).click()
        page.screenshot(path="pw_screens/06_after_import.png", full_page=True)

        # 6) Crear
        first_visible(page, ['button:has-text("CREAR")', 'button:has-text("Crear")']).click()
        page.wait_for_load_state("networkidle")
        page.screenshot(path="pw_screens/07_after_create.png", full_page=True)

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


