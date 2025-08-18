# parrotfy_sync.py
import os, re, json
from datetime import datetime
from typing import Dict, List, Tuple

import gspread
from google.oauth2.credentials import Credentials
from playwright.sync_api import sync_playwright

# ---------- Config/env ----------
SPREADSHEET_ID   = os.environ["SPREADSHEET_ID"]
DATA_SHEET_NAME  = os.environ.get("DATA_SHEET_NAME", "OCR Recepciones")
PRICES_SHEET     = os.environ.get("PRICES_SHEET_NAME", "Precios")
STRICT_PRICES    = os.environ.get("STRICT_PRICES", "1") in ("1", "true", "True")

PARROTFY_URL  = os.environ["PARROTFY_URL"].rstrip("/")
PARROTFY_USER = os.environ["PARROTFY_USER"]
PARROTFY_PASS = os.environ["PARROTFY_PASS"]

# ---------- Google helpers ----------
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
    idx_price = header.index("precio") if "precio" in header else header.index("precios")
    price_map: Dict[str, float] = {}
    for r in rows[1:]:
        if len(r) <= max(idx_sku, idx_price):
            continue
        sku = re.sub(r"\D", "", r[idx_sku])
        if not sku:
            continue
        try:
            price = float(str(r[idx_price]).replace(",", "."))
        except:
            continue
        price_map[sku] = price
    return price_map

def pick_rows(ws_data) -> Tuple[List[List[str]], List[int]]:
    rows = ws_data.get_all_values()
    if not rows:
        return [], []
    header = [h.strip().lower() for h in rows[0]]
    idx_sku  = header.index("sku")
    idx_unr  = header.index("un_recibidas")
    idx_flag = header.index("parrotfy_enviado") if "parrotfy_enviado" in header else None

    pending: List[List[str]] = []
    row_indexes: List[int] = []
    for i, r in enumerate(rows[1:], start=2):  # gspread 1-index; header es fila 1
        if len(r) <= max(idx_sku, idx_unr):
            continue
        if idx_flag is not None and len(r) > idx_flag and str(r[idx_flag]).strip():
            continue
        sku = re.sub(r"\D", "", r[idx_sku])
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
        ws_data.update_cell(1, len(header) + 1, "parrotfy_enviado")
        col = len(header) + 1
    else:
        col = header.index("parrotfy_enviado") + 1

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updates = [{"range": gspread.utils.rowcol_to_a1(r, col), "values": [[ts]]} for r in row_indexes]
    ws_data.batch_update(updates, value_input_option="RAW")

# ---------- Playwright helpers ----------
def first_visible(page, selectors, timeout=3000):
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=timeout)
            return loc
        except:
            continue
    raise RuntimeError(f"Ningún selector visible: {selectors}")

def click_import_button(page):
    # Asegurar el formulario a la vista
    try:
        page.locator('xpath=//*[@id="new_inventory_movement_group"]').scroll_into_view_if_needed()
    except:
        pass

    # 1) Tu XPath al <a>
    try:
        page.click('xpath=//*[@id="new_inventory_movement_group"]/div[1]/div[5]/div/a[1]', timeout=1500)
        return True
    except:
        pass
    # 2) Tu XPath al <i>
    try:
        page.click('xpath=//*[@id="new_inventory_movement_group"]/div[1]/div[5]/div/a[1]/i', timeout=1500)
        return True
    except:
        pass
    # 3) Evaluación JS: buscar cualquier <a>/<button> con texto "Importar lista"
    try:
        clicked = page.evaluate("""
            (() => {
              const els = Array.from(document.querySelectorAll('a,button,[role=button]'));
              const el = els.find(e => /importar lista/i.test(e.textContent || ''));
              if (el) { el.click(); return true; }
              return false;
            })();
        """)
        if clicked:
            return True
    except:
        pass
    # 4) Genéricos + scroll + menús
    selectors = [
        'button[aria-label*="Importar" i]',
        'button[title*="Importar" i]',
        '[data-tooltip*="Importar" i]',
        'button:has-text("Importar lista de movimientos")',
        'button:has-text("Importar lista")',
        'a:has-text("Importar lista de movimientos")',
        'a:has-text("Importar lista")',
        'text=/\\bImportar lista\\b/i',
        'text=/\\bImportar\\b/i',
    ]
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=1200)
            return True
        except:
            pass
    try:
        page.mouse.wheel(0, 2000); page.wait_for_timeout(400)
    except:
        pass
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=1200)
            return True
        except:
            pass
    for more in ['button:has-text("Acciones")','button:has-text("Más")','[aria-haspopup="menu"]','button:has-text("⋯")','button:has-text("...")']:
        try:
            page.locator(more).first.click(timeout=800)
            for sel in selectors:
                try:
                    page.locator(sel).first.click(timeout=800)
                    return True
                except:
                    pass
        except:
            continue
    return False

# ---------- Construir bloque a pegar ----------
def build_import_text(pending_rows: List[List[str]], price_map: Dict[str, float]):
    lines, missing, triples = [], [], []
    for sku, qty in pending_rows:
        price = price_map.get(sku)
        if price is None:
            if STRICT_PRICES:
                missing.append(sku)
                continue
            price = 0.0
        price_str = str(int(price)) if float(price).is_integer() else str(price)
        lines.append(f"{sku}\t{qty}\t{price_str}")
        triples.append((sku, qty, price_str))
    return "\n".join(lines), missing, triples

# ---------- Fallback manual (sin modal) ----------
def manual_add_rows(page, triples):
    def find_input(candidates, to=None):
        return first_visible(page, candidates, timeout=to or 2500)

    for (sku, qty, price) in triples:
        # Producto
        prod = find_input([
            'input[role="combobox"]',
            'input[placeholder*="Producto" i]',
            'input[name*="product" i]',
            '[aria-label*="Producto" i]',
            '//label[contains(., "Producto")]/following::input[1]',
        ])
        prod.click(); prod.fill(sku); page.keyboard.press("Enter")

        # Cantidad
        qty_in = find_input([
            'input[name*="cantidad" i]',
            'input[name*="quantity" i]',
            'input[placeholder*="Cantidad" i]',
            'input[type="number"]',
            '//label[contains(., "Cantidad")]/following::input[1]',
        ])
        qty_in.click(); qty_in.fill(str(qty))

        # Precio
        price_in = find_input([
            'input[name*="precio" i]',
            'input[name*="valor" i]',
            'input[placeholder*="Precio" i]',
            'input[placeholder*="Valor" i]',
            '//label[contains(., "Precio") or contains(., "Valor")]/following::input[1]',
        ])
        price_in.click(); price_in.fill(str(price))

        # Confirmar fila
        try:
            page.keyboard.press("Enter")
        except:
            for add_sel in ['button:has-text("Agregar")','button:has-text("Añadir")','button:has-text("+")']:
                try:
                    page.locator(add_sel).first.click(timeout=800); break
                except:
                    continue
        page.wait_for_timeout(250)

# ---------- Flujo Parrotfy ----------
def run_parrotfy_import(import_text: str, triples=None):
    from pathlib import Path
    Path("pw_screens").mkdir(exist_ok=True, parents=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # Ir directo (redirige a login si no hay sesión)
        page.goto(f"{PARROTFY_URL}/inventory_movement_groups/new", wait_until="domcontentloaded")
        page.screenshot(path="pw_screens/00_initial.png", full_page=True)

        # Login tolerante
        try:
            email_input = first_visible(page, [
                'input[name="user[email]"]','input[type="email"]','input[name*="email" i]',
                '[placeholder*="mail" i]','//input[@type="email"]',
                '//label[contains(., "Email") or contains(., "Correo")]/following::input[1]',
                'input[name$="[email]"]',
            ], timeout=2000)

            for btn in ['button:has-text("Aceptar")','button:has-text("Accept")','text=Aceptar']:
                try: page.click(btn, timeout=1000); break
                except: pass

            email_input.fill(PARROTFY_USER)
            password_input = first_visible(page, [
                'input[name="user[password]"]','input[type="password"]','//input[@type="password"]',
                '//label[contains(., "Contraseña") or contains(., "Password")]/following::input[1]',
                'input[name$="[password]"]',
            ])
            password_input.fill(PARROTFY_PASS)

            first_visible(page, [
                'button[type="submit"]','input[type="submit"]',
                'button:has-text("Iniciar")','button:has-text("Entrar")','button:has-text("Sign in")',
            ]).click()

            page.wait_for_load_state("networkidle")
            page.screenshot(path="pw_screens/01_after_login.png", full_page=True)
        except:
            page.screenshot(path="pw_screens/01_no_login.png", full_page=True)

        # Asegurar página target
        page.goto(f"{PARROTFY_URL}/inventory_movement_groups/new", wait_until="domcontentloaded")
        page.screenshot(path="pw_screens/02_new_page.png", full_page=True)
        with open("pw_screens/02_new_page.html","w",encoding="utf-8") as f:
            f.write(page.content())

        # Campos fijos
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

        # Intentar abrir Importar lista
        opened = click_import_button(page)
        if opened:
            # Esperar modal: role=dialog, aria-modal, o clases típicas
            dlg = None
            for sel in ['role=dialog', 'div[role="dialog"]', '[aria-modal="true"]', '.modal.show', '.modal[open]']:
                try:
                    dlg = page.locator(sel).first
                    dlg.wait_for(state="visible", timeout=4000)
                    break
                except:
                    continue
            if dlg is None:
                page.screenshot(path="pw_screens/04_no_modal.png", full_page=True)
                raise RuntimeError("Se hizo click en Importar, pero no apareció el modal.")

            page.screenshot(path="pw_screens/05_modal_open.png", full_page=True)
            # Pegar bloque
            try:
                area = dlg.locator("textarea").first
                area.click(); area.fill(import_text)
            except:
                ce = dlg.locator("[contenteditable=true]").first
                ce.click(); ce.type(import_text)
            dlg.get_by_role("button", name=re.compile("importar", re.I)).click()
            page.screenshot(path="pw_screens/06_after_import.png", full_page=True)
        else:
            # Dump de controles y fallback manual
            try:
                texts = page.locator("button, a, [role=button]").all_text_contents()
                with open("pw_screens/04_controls.txt","w",encoding="utf-8") as f:
                    f.write("\n".join([t.strip() for t in texts if t.strip()]))
            except:
                pass
            if not triples:
                raise RuntimeError("No hallé el botón 'Importar lista' y no tengo datos para fallback manual.")
            manual_add_rows(page, triples)
            page.screenshot(path="pw_screens/06_after_manual.png", full_page=True)

        # Crear
        first_visible(page, ['button:has-text("CREAR")','button:has-text("Crear")']).click()
        page.wait_for_load_state("networkidle")
        page.screenshot(path="pw_screens/07_after_create.png", full_page=True)

        ctx.close(); browser.close()

# ---------- Main ----------
def main():
    print("[SYNC] Abriendo Google Sheet...")
    _, ws_data, ws_prices = open_sheet()

    print("[SYNC] Leyendo precios...")
    price_map = read_prices(ws_prices)

    print("[SYNC] Leyendo filas pendientes...")
    pending_rows, row_ids = pick_rows(ws_data)
    print(f"[SYNC] Filas con un_recibidas>0 y no enviadas: {len(pending_rows)}")
    if not pending_rows:
        print("[SYNC] Nada que enviar. Fin."); return

    import_text, missing, triples = build_import_text(pending_rows, price_map)
    if missing:
        print(f"[SYNC] Falta precio para {len(missing)} SKU(s). STRICT={STRICT_PRICES}")
        for s in missing[:20]: print("   -", s)
        if STRICT_PRICES and not import_text.strip():
            print("[SYNC] No quedó ninguna fila válida. Fin."); return

    print("[SYNC] Abriendo Parrotfy e importando...")
    run_parrotfy_import(import_text, triples=triples)

    print("[SYNC] Marcando filas como enviadas...")
    mark_sent(ws_data, row_ids)
    print("✅ Listo.")

if __name__ == "__main__":
    main()
