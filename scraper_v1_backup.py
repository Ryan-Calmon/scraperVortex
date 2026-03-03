"""
Scraper Vortx DCM - Coleta de Emissões de Títulos de Dívida
=============================================================
Coleta dados de emissões (CRA, CRI, Debêntures, etc.) do site da Vortx,
incluindo histórico de PU e download de documentos PDF.

Uso:
    python scraper.py                  # Execução completa
    python scraper.py --update-pu      # Apenas atualizar PU (últimos 5 dias)
    python scraper.py --skip-docs      # Pular download de documentos
    python scraper.py --max-pages 5    # Limitar a N páginas (teste)
"""

import os
import re
import sys
import time
import sqlite3
import logging
import argparse
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException,
)

# ─────────────────────────────────────────────────────────────────────
# Configurações
# ─────────────────────────────────────────────────────────────────────
BASE_URL = "https://www.vortx.com.br/investidor/dcm"
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "vortx_dcm.db"
DOCS_DIR = SCRIPT_DIR / "documentos"
LOG_DIR = SCRIPT_DIR / "logs"

PAGE_LOAD_TIMEOUT = 30
ELEMENT_WAIT_TIMEOUT = 15
REQUEST_DELAY = 1.5          # Delay entre requisições (segundos)
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"scraper_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Banco de Dados
# ─────────────────────────────────────────────────────────────────────
def init_database(db_path: Path) -> sqlite3.Connection:
    """Inicializa o banco SQLite com as tabelas necessárias."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS emissoes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_if           TEXT,
            codigo_isin         TEXT,
            tipo                TEXT,
            nome                TEXT,
            emissor             TEXT,
            cnpj_emissor        TEXT,
            numero_emissao      TEXT,
            serie               TEXT,
            trustee             TEXT,
            volume_emitido      TEXT,
            data_emissao        TEXT,
            data_vencimento     TEXT,
            url_detalhe         TEXT,
            atualizado_em       TEXT,
            UNIQUE(codigo_if, codigo_isin, numero_emissao, serie)
        );

        CREATE TABLE IF NOT EXISTS historico_pu (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            emissao_id      INTEGER NOT NULL,
            data            TEXT NOT NULL,
            valor_pu        TEXT NOT NULL,
            atualizado_em   TEXT,
            FOREIGN KEY (emissao_id) REFERENCES emissoes(id),
            UNIQUE(emissao_id, data)
        );

        CREATE TABLE IF NOT EXISTS documentos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            emissao_id      INTEGER NOT NULL,
            categoria       TEXT NOT NULL,
            nome_arquivo    TEXT NOT NULL,
            url_download    TEXT,
            caminho_local   TEXT,
            baixado         INTEGER DEFAULT 0,
            atualizado_em   TEXT,
            FOREIGN KEY (emissao_id) REFERENCES emissoes(id),
            UNIQUE(emissao_id, categoria, nome_arquivo)
        );

        CREATE INDEX IF NOT EXISTS idx_emissoes_codigo_if
            ON emissoes(codigo_if);
        CREATE INDEX IF NOT EXISTS idx_emissoes_vencimento
            ON emissoes(data_vencimento);
        CREATE INDEX IF NOT EXISTS idx_pu_emissao_data
            ON historico_pu(emissao_id, data);
    """)
    conn.commit()
    logger.info("Banco de dados inicializado: %s", db_path)
    return conn


def upsert_emissao(conn: sqlite3.Connection, data: dict) -> int:
    """Insere ou atualiza uma emissão. Retorna o ID."""
    now = datetime.now().isoformat()
    cur = conn.execute("""
        INSERT INTO emissoes (
            codigo_if, codigo_isin, tipo, nome, emissor, cnpj_emissor,
            numero_emissao, serie, trustee, volume_emitido,
            data_emissao, data_vencimento, url_detalhe, atualizado_em
        ) VALUES (
            :codigo_if, :codigo_isin, :tipo, :nome, :emissor, :cnpj_emissor,
            :numero_emissao, :serie, :trustee, :volume_emitido,
            :data_emissao, :data_vencimento, :url_detalhe, :atualizado_em
        )
        ON CONFLICT(codigo_if, codigo_isin, numero_emissao, serie)
        DO UPDATE SET
            tipo           = excluded.tipo,
            nome           = excluded.nome,
            emissor        = excluded.emissor,
            cnpj_emissor   = excluded.cnpj_emissor,
            trustee        = excluded.trustee,
            volume_emitido = excluded.volume_emitido,
            data_emissao   = COALESCE(excluded.data_emissao, data_emissao),
            data_vencimento= COALESCE(excluded.data_vencimento, data_vencimento),
            url_detalhe    = excluded.url_detalhe,
            atualizado_em  = excluded.atualizado_em
    """, {**data, "atualizado_em": now})
    conn.commit()

    # Recuperar o ID
    row = conn.execute("""
        SELECT id FROM emissoes
        WHERE COALESCE(codigo_if, '') = COALESCE(:codigo_if, '')
          AND COALESCE(codigo_isin, '') = COALESCE(:codigo_isin, '')
          AND COALESCE(numero_emissao, '') = COALESCE(:numero_emissao, '')
          AND COALESCE(serie, '') = COALESCE(:serie, '')
    """, data).fetchone()
    return row[0] if row else cur.lastrowid


def upsert_pu(conn: sqlite3.Connection, emissao_id: int, data: str, valor: str):
    """Insere ou atualiza um registro de PU."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO historico_pu (emissao_id, data, valor_pu, atualizado_em)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(emissao_id, data)
        DO UPDATE SET valor_pu = excluded.valor_pu, atualizado_em = excluded.atualizado_em
    """, (emissao_id, data, valor, now))


def upsert_documento(conn: sqlite3.Connection, emissao_id: int,
                     categoria: str, nome: str, url: str, caminho: str):
    """Insere ou atualiza um registro de documento."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO documentos (emissao_id, categoria, nome_arquivo, url_download, caminho_local, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(emissao_id, categoria, nome_arquivo)
        DO UPDATE SET url_download = excluded.url_download,
                      caminho_local = excluded.caminho_local,
                      atualizado_em = excluded.atualizado_em
    """, (emissao_id, categoria, nome, url, caminho, now))


def get_emissoes_para_atualizar_pu(conn: sqlite3.Connection):
    """Retorna emissões não vencidas para atualização de PU."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    return conn.execute("""
        SELECT id, codigo_if, codigo_isin, url_detalhe, nome
        FROM emissoes
        WHERE data_vencimento IS NULL
           OR data_vencimento = ''
           OR data_vencimento >= ?
    """, (hoje,)).fetchall()


def get_all_emissoes(conn: sqlite3.Connection):
    """Retorna todas as emissões."""
    return conn.execute("""
        SELECT id, codigo_if, codigo_isin, url_detalhe, nome, data_vencimento
        FROM emissoes
    """).fetchall()


# ─────────────────────────────────────────────────────────────────────
# Selenium WebDriver
# ─────────────────────────────────────────────────────────────────────
def create_driver() -> webdriver.Chrome:
    """Cria e configura o Chrome WebDriver."""
    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={USER_AGENT}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Configurar diretório de download para PDFs
    prefs = {
        "download.default_directory": str(DOCS_DIR),
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    # Mascarar detecção de automação
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    logger.info("Chrome WebDriver inicializado (headless).")
    return driver


def safe_click(driver, element, timeout: int = 5):
    """Clica em um elemento com tratamento de interceptação."""
    try:
        element.click()
    except ElementClickInterceptedException:
        time.sleep(1)
        driver.execute_script("arguments[0].click();", element)


def wait_and_find(driver, by, value, timeout=ELEMENT_WAIT_TIMEOUT):
    """Espera e retorna um elemento."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def wait_and_find_all(driver, by, value, timeout=ELEMENT_WAIT_TIMEOUT):
    """Espera e retorna todos os elementos."""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )
    return driver.find_elements(by, value)


def dismiss_cookies(driver):
    """Tenta fechar o banner de cookies, se presente."""
    try:
        btns = driver.find_elements(By.XPATH,
            "//button[contains(text(),'Aceitar') or contains(text(),'Accept') "
            "or contains(text(),'Rejeitar') or contains(text(),'Reject')]"
        )
        for btn in btns:
            if btn.is_displayed():
                safe_click(driver, btn)
                logger.info("Banner de cookies fechado.")
                time.sleep(0.5)
                return
    except Exception:
        pass


def close_chat_widget(driver):
    """Tenta fechar widget de chat, se presente."""
    try:
        # Zendesk / Intercom / genérico
        for selector in [
            "button[aria-label='Close']",
            ".intercom-launcher",
            "#launcher",
            "iframe[title*='chat']",
        ]:
            els = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in els:
                if el.is_displayed():
                    driver.execute_script("arguments[0].style.display='none'", el)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────
def sanitize_filename(name: str) -> str:
    """Remove caracteres inválidos de nomes de arquivo."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:200]  # Limitar tamanho


def parse_date(date_str: str) -> Optional[str]:
    """Converte data BR (dd/mm/yyyy) para ISO (yyyy-mm-dd)."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_cnpj(text: str) -> tuple[str, str]:
    """Extrai nome do emissor e CNPJ de uma string como 'EMPRESA SA 12.345.678/0001-90'."""
    cnpj_pattern = r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})'
    match = re.search(cnpj_pattern, text)
    if match:
        cnpj = match.group(1)
        nome = text[:match.start()].strip()
        return nome, cnpj
    return text.strip(), ""


def retry(func, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, **kwargs):
    """Executa uma função com tentativas de retry."""
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            logger.warning("Tentativa %d/%d falhou: %s", attempt, attempts, e)
            if attempt < attempts:
                time.sleep(delay)
    logger.error("Todas as %d tentativas falharam. Ultimo erro: %s", attempts, last_err)
    raise last_err


# ─────────────────────────────────────────────────────────────────────
# Scraping - Página Principal (Listagem)
# ─────────────────────────────────────────────────────────────────────
def scrape_listing_page(driver, page_num: int) -> list[dict]:
    """Extrai dados das emissões de uma página de listagem."""
    url = f"{BASE_URL}?pagina={page_num}"
    logger.info("Acessando pagina %d: %s", page_num, url)
    driver.get(url)
    time.sleep(REQUEST_DELAY)

    dismiss_cookies(driver)
    close_chat_widget(driver)

    emissoes = []

    # Aguardar a tabela carregar
    try:
        WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr, .card, [class*='emission'], [class*='list']"))
        )
    except TimeoutException:
        logger.warning("Timeout ao carregar listagem na pagina %d", page_num)
        # Tentar com espera extra
        time.sleep(5)

    # Estratégia 1: Tabela HTML padrão
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    if rows:
        logger.info("Encontradas %d linhas na tabela (pagina %d)", len(rows), page_num)
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 5:
                    continue

                texts = [c.text.strip() for c in cells]

                # Estrutura observada: Tipo | Nome | Código | Emissão | Série | Emissor+CNPJ | Trustee | Volume
                tipo = texts[0] if len(texts) > 0 else ""
                nome = texts[1] if len(texts) > 1 else ""
                codigo = texts[2] if len(texts) > 2 else ""
                num_emissao = texts[3] if len(texts) > 3 else ""
                serie = texts[4] if len(texts) > 4 else ""
                emissor_raw = texts[5] if len(texts) > 5 else ""
                trustee = texts[6] if len(texts) > 6 else ""
                volume = texts[7] if len(texts) > 7 else ""

                emissor_nome, cnpj = extract_cnpj(emissor_raw)

                # Tentar extrair link de detalhes
                url_detalhe = ""
                try:
                    link_el = row.find_element(By.CSS_SELECTOR, "a[href*='dcm'], a[href*='detalhe'], a")
                    url_detalhe = link_el.get_attribute("href") or ""
                except NoSuchElementException:
                    pass

                # Determinar identificador
                codigo_if = codigo if codigo else ""
                codigo_isin = ""

                # Se o código parece ser ISIN (começa com BR e tem 12 chars)
                if codigo_if and re.match(r'^BR[A-Z0-9]{10}$', codigo_if):
                    codigo_isin = codigo_if
                    codigo_if = ""

                emissoes.append({
                    "codigo_if": codigo_if,
                    "codigo_isin": codigo_isin,
                    "tipo": tipo,
                    "nome": nome,
                    "emissor": emissor_nome,
                    "cnpj_emissor": cnpj,
                    "numero_emissao": num_emissao,
                    "serie": serie,
                    "trustee": trustee,
                    "volume_emitido": volume,
                    "data_emissao": None,
                    "data_vencimento": None,
                    "url_detalhe": url_detalhe,
                })
            except StaleElementReferenceException:
                logger.warning("Elemento ficou stale ao processar linha.")
                continue
            except Exception as e:
                logger.warning("Erro ao processar linha: %s", e)
                continue
        return emissoes

    # Estratégia 2: Cards ou divs (caso não seja tabela)
    cards = driver.find_elements(By.CSS_SELECTOR,
        "[class*='card'], [class*='emission-item'], [class*='list-item'], "
        "[class*='Card'], [class*='Item']"
    )
    if cards:
        logger.info("Encontrados %d cards (pagina %d)", len(cards), page_num)
        for card in cards:
            try:
                text = card.text.strip()
                if not text or len(text) < 10:
                    continue

                lines = [l.strip() for l in text.split('\n') if l.strip()]

                url_detalhe = ""
                try:
                    link = card.find_element(By.TAG_NAME, "a")
                    url_detalhe = link.get_attribute("href") or ""
                except NoSuchElementException:
                    pass

                # Parse básico do card
                tipo = ""
                nome = ""
                codigo = ""
                emissor_nome = ""
                cnpj = ""
                volume = ""

                for line in lines:
                    line_lower = line.lower()
                    if any(t in line_lower for t in ['cri', 'cra', 'debênture', 'debenture', 'lci', 'lca']):
                        if not tipo:
                            tipo = line
                    elif re.search(r'R\$\s*[\d.,]+', line):
                        volume = line
                    elif re.search(r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}', line):
                        emissor_nome, cnpj = extract_cnpj(line)
                    elif re.match(r'^[A-Z0-9]{8,}$', line):
                        codigo = line
                    elif not nome and line not in ['Ver detalhes', 'detalhes']:
                        nome = line

                if nome or codigo:
                    codigo_if = codigo
                    codigo_isin = ""
                    if codigo_if and re.match(r'^BR[A-Z0-9]{10}$', codigo_if):
                        codigo_isin = codigo_if
                        codigo_if = ""

                    emissoes.append({
                        "codigo_if": codigo_if,
                        "codigo_isin": codigo_isin,
                        "tipo": tipo,
                        "nome": nome,
                        "emissor": emissor_nome,
                        "cnpj_emissor": cnpj,
                        "numero_emissao": "",
                        "serie": "",
                        "trustee": "",
                        "volume_emitido": volume,
                        "data_emissao": None,
                        "data_vencimento": None,
                        "url_detalhe": url_detalhe,
                    })
            except Exception as e:
                logger.warning("Erro ao processar card: %s", e)
                continue

    # Estratégia 3: XPath genérico para linhas de dados
    if not emissoes:
        logger.info("Tentando estrategia alternativa de extracao (pagina %d)", page_num)
        all_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='dcm']")
        for link in all_links:
            href = link.get_attribute("href") or ""
            text = link.text.strip()
            if href and href != BASE_URL and "pagina=" not in href and text:
                parent = link.find_element(By.XPATH, "./..")
                parent_text = parent.text.strip()
                emissoes.append({
                    "codigo_if": text if not re.match(r'^BR', text) else "",
                    "codigo_isin": text if re.match(r'^BR', text) else "",
                    "tipo": "",
                    "nome": text,
                    "emissor": "",
                    "cnpj_emissor": "",
                    "numero_emissao": "",
                    "serie": "",
                    "trustee": "",
                    "volume_emitido": "",
                    "data_emissao": None,
                    "data_vencimento": None,
                    "url_detalhe": href,
                })

    logger.info("Extraidas %d emissoes na pagina %d", len(emissoes), page_num)
    return emissoes


def get_total_pages(driver) -> int:
    """Determina o número total de páginas na listagem."""
    driver.get(BASE_URL)
    time.sleep(REQUEST_DELAY)
    dismiss_cookies(driver)

    # Procurar pelo último número de página na paginação
    try:
        # Buscar links de paginação
        pag_links = driver.find_elements(By.CSS_SELECTOR,
            "a[href*='pagina='], [class*='pagination'] a, [class*='pager'] a, nav a"
        )
        max_page = 1
        for link in pag_links:
            href = link.get_attribute("href") or ""
            text = link.text.strip()
            # Extrair número da página do href
            match = re.search(r'pagina=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
            # Ou do texto se for numérico
            if text.isdigit():
                max_page = max(max_page, int(text))

        logger.info("Total de paginas detectado: %d", max_page)
        return max_page
    except Exception as e:
        logger.warning("Nao foi possivel detectar total de paginas: %s. Usando 807.", e)
        return 807


def scrape_all_listings(driver, conn: sqlite3.Connection,
                        max_pages: Optional[int] = None) -> list[int]:
    """Coleta todas as emissões da listagem e salva no banco."""
    total_pages = get_total_pages(driver)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    logger.info("Iniciando coleta de %d paginas de listagem.", total_pages)
    all_ids = []
    total_emissoes = 0

    for page in range(1, total_pages + 1):
        try:
            emissoes = retry(scrape_listing_page, driver, page)
            for em in emissoes:
                em_id = upsert_emissao(conn, em)
                all_ids.append(em_id)
                total_emissoes += 1

            if page % 50 == 0:
                logger.info("Progresso: %d/%d paginas, %d emissoes coletadas.",
                            page, total_pages, total_emissoes)

        except Exception as e:
            logger.error("Erro na pagina %d: %s. Continuando...", page, e)
            continue

    logger.info("Coleta de listagem completa: %d emissoes em %d paginas.",
                total_emissoes, total_pages)
    return all_ids


# ─────────────────────────────────────────────────────────────────────
# Scraping - Página de Detalhes
# ─────────────────────────────────────────────────────────────────────
def scrape_detail_page(driver, url: str) -> dict:
    """Extrai informacoes detalhadas de uma emissao.

    A pagina de detalhes da Vortx tem a seguinte estrutura:
    - Cabecalho: campos como 'Codigo IF', 'Codigo ISIN', 'Emissor' com labels
      e valores em elementos adjacentes (geralmente <span> ou <p>).
    - Aba 'Informacoes' (padrao): tabela com linhas label|valor, incluindo
      'Data de Emissao', 'Data de Vencimento', 'Remuneracao', etc.
    """
    result = {
        "data_emissao": None,
        "data_vencimento": None,
        "codigo_isin": None,
        "codigo_if": None,
    }

    if not url:
        return result

    logger.debug("Acessando detalhes: %s", url)
    driver.get(url)
    time.sleep(REQUEST_DELAY)
    dismiss_cookies(driver)
    close_chat_widget(driver)

    try:
        WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except TimeoutException:
        logger.warning("Timeout ao carregar pagina de detalhes: %s", url)
        return result

    # --- Estrategia 1: Extrair de TODAS as tabelas (th/td e td/td) -----------
    # A secao "INFORMACOES PRINCIPAIS" usa <table> com pares label|valor.
    try:
        tables = driver.find_elements(By.TAG_NAME, "table")
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                headers = row.find_elements(By.TAG_NAME, "th")
                # Normalizar: label pode ser <th> ou primeiro <td>
                label = ""
                value = ""
                if headers and cells:
                    label = headers[0].text.strip().lower()
                    value = cells[0].text.strip()
                elif len(cells) >= 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                else:
                    continue

                if not label or not value:
                    continue

                if "data de emiss" in label and not result["data_emissao"]:
                    dm = re.search(r'(\d{2}/\d{2}/\d{4})', value)
                    if dm:
                        result["data_emissao"] = parse_date(dm.group(1))

                elif "vencimento" in label and not result["data_vencimento"]:
                    dm = re.search(r'(\d{2}/\d{2}/\d{4})', value)
                    if dm:
                        result["data_vencimento"] = parse_date(dm.group(1))

                elif "isin" in label and not result["codigo_isin"]:
                    im = re.search(r'([A-Z]{2}[A-Z0-9]{10})', value)
                    if im:
                        result["codigo_isin"] = im.group(1)

                elif "digo if" in label and not result["codigo_if"]:
                    if value and len(value) > 2:
                        result["codigo_if"] = value.strip()
    except Exception as e:
        logger.debug("Erro ao extrair de tabelas: %s", e)

    # --- Estrategia 2: Cabeçalho com pares label/valor adjacentes -------------
    # Estrutura: <span class=label>Codigo ISIN</span><span class=value>BR...</span>
    try:
        all_elements = driver.find_elements(By.CSS_SELECTOR,
            "span, p, div.field, div.info-value, dd, dt, label"
        )
        for i, el in enumerate(all_elements):
            text = el.text.strip().lower()
            if not text:
                continue
            # Encontrar o proximo elemento com valor
            next_val = ""
            for j in range(i + 1, min(i + 4, len(all_elements))):
                candidate = all_elements[j].text.strip()
                if candidate and candidate.lower() != text:
                    next_val = candidate
                    break

            if "data de emiss" in text and not result["data_emissao"]:
                dm = re.search(r'(\d{2}/\d{2}/\d{4})', next_val)
                if dm:
                    result["data_emissao"] = parse_date(dm.group(1))

            elif "data de vencimento" in text and not result["data_vencimento"]:
                dm = re.search(r'(\d{2}/\d{2}/\d{4})', next_val)
                if dm:
                    result["data_vencimento"] = parse_date(dm.group(1))

            elif "digo isin" in text and not result["codigo_isin"]:
                im = re.search(r'([A-Z]{2}[A-Z0-9]{10})', next_val)
                if im:
                    result["codigo_isin"] = im.group(1)

            elif "digo if" in text and not result["codigo_if"]:
                if next_val and len(next_val) > 2 and not next_val.lower().startswith("c"):
                    result["codigo_if"] = next_val.strip()
    except Exception as e:
        logger.debug("Erro ao extrair de elementos adjacentes: %s", e)

    # --- Estrategia 3: Fallback via texto completo da pagina ------------------
    if not result["data_emissao"] or not result["data_vencimento"]:
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text

            if not result["data_emissao"]:
                m = re.search(
                    r'Data\s+de\s+Emiss[aã]o\s*[:\s]*(\d{2}/\d{2}/\d{4})',
                    page_text, re.IGNORECASE
                )
                if m:
                    result["data_emissao"] = parse_date(m.group(1))

            if not result["data_vencimento"]:
                m = re.search(
                    r'(?:Data\s+de\s+)?Vencimento\s*[:\s]*(\d{2}/\d{2}/\d{4})',
                    page_text, re.IGNORECASE
                )
                if m:
                    result["data_vencimento"] = parse_date(m.group(1))

            if not result["codigo_isin"]:
                m = re.search(
                    r'(?:C[oó]digo\s+)?ISIN\s*[:\s]*([A-Z]{2}[A-Z0-9]{10})',
                    page_text, re.IGNORECASE
                )
                if m:
                    result["codigo_isin"] = m.group(1)
        except Exception as e:
            logger.debug("Erro no fallback via texto: %s", e)

    # --- Estrategia 4: Clicar na aba "Informacoes" se nada foi encontrado -----
    if not result["data_emissao"] or not result["data_vencimento"]:
        try:
            for sel in [
                "//button[contains(text(),'Informa')]",
                "//a[contains(text(),'Informa')]",
                "//span[contains(text(),'Informa')]",
                "//*[contains(@class,'tab')][contains(text(),'Informa')]",
            ]:
                tabs = driver.find_elements(By.XPATH, sel)
                for t in tabs:
                    if t.is_displayed() and "informa" in t.text.strip().lower():
                        safe_click(driver, t)
                        time.sleep(2)
                        break
                else:
                    continue
                break

            # Re-tentar extrair das tabelas apos clicar na aba
            tables = driver.find_elements(By.TAG_NAME, "table")
            for table in tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    headers = row.find_elements(By.TAG_NAME, "th")
                    label = ""
                    value = ""
                    if headers and cells:
                        label = headers[0].text.strip().lower()
                        value = cells[0].text.strip()
                    elif len(cells) >= 2:
                        label = cells[0].text.strip().lower()
                        value = cells[1].text.strip()
                    else:
                        continue

                    if "data de emiss" in label and not result["data_emissao"]:
                        dm = re.search(r'(\d{2}/\d{2}/\d{4})', value)
                        if dm:
                            result["data_emissao"] = parse_date(dm.group(1))
                    elif "vencimento" in label and not result["data_vencimento"]:
                        dm = re.search(r'(\d{2}/\d{2}/\d{4})', value)
                        if dm:
                            result["data_vencimento"] = parse_date(dm.group(1))
        except Exception as e:
            logger.debug("Erro ao clicar na aba Informacoes: %s", e)

    logger.debug("Detalhes extraidos para %s: emissao=%s vencimento=%s isin=%s if=%s",
                 url, result["data_emissao"], result["data_vencimento"],
                 result["codigo_isin"], result["codigo_if"])
    return result


def scrape_pu_history(driver, url: str, only_recent: bool = False) -> list[dict]:
    """Extrai o histórico de PU da aba PU na página de detalhes."""
    pu_data = []
    if not url:
        return pu_data

    driver.get(url)
    time.sleep(REQUEST_DELAY)
    dismiss_cookies(driver)
    close_chat_widget(driver)

    # Clicar na aba PU
    try:
        pu_tab = None
        # Tentar vários seletores para a aba PU
        for selector in [
            "//button[contains(text(),'PU')]",
            "//a[contains(text(),'PU')]",
            "//span[contains(text(),'PU')]",
            "//li[contains(text(),'PU')]",
            "//div[contains(text(),'PU')]",
            "//*[@data-tab='pu' or @data-tab='PU']",
            "//*[contains(@class,'tab')][contains(text(),'PU')]",
        ]:
            tabs = driver.find_elements(By.XPATH, selector)
            for t in tabs:
                tab_text = t.text.strip()
                # Garantir que é exatamente "PU" e não outro texto contendo PU
                if tab_text == "PU" or tab_text.upper() == "PU":
                    pu_tab = t
                    break
            if pu_tab:
                break

        if pu_tab:
            safe_click(driver, pu_tab)
            time.sleep(2)
            logger.debug("Aba PU clicada.")
        else:
            logger.debug("Aba PU nao encontrada para: %s", url)
            return pu_data

    except Exception as e:
        logger.debug("Erro ao clicar na aba PU: %s", e)
        return pu_data

    # Extrair dados de PU da tabela
    cutoff_date = None
    if only_recent:
        cutoff_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    max_scroll_attempts = 50 if not only_recent else 5
    seen_dates = set()

    for scroll_attempt in range(max_scroll_attempts):
        try:
            rows = driver.find_elements(By.CSS_SELECTOR,
                "table tbody tr, [class*='pu'] tr, [class*='PU'] tr"
            )
            if not rows:
                # Tentar via divs ou spans para layout não-tabular
                rows = driver.find_elements(By.CSS_SELECTOR,
                    "[class*='pu-row'], [class*='pu-item'], [class*='price-row']"
                )

            new_data = False
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        cells = row.find_elements(By.CSS_SELECTOR, "span, div")

                    if len(cells) >= 2:
                        date_text = cells[0].text.strip()
                        pu_text = cells[1].text.strip()

                        # Validar data
                        parsed_date = parse_date(date_text)
                        if not parsed_date:
                            continue

                        # Se só precisa dos recentes, filtrar
                        if cutoff_date and parsed_date < cutoff_date:
                            continue

                        if parsed_date not in seen_dates:
                            seen_dates.add(parsed_date)
                            pu_data.append({
                                "data": parsed_date,
                                "valor_pu": pu_text,
                            })
                            new_data = True
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    continue

            # Tentar scroll/paginação da tabela de PU
            if not only_recent and new_data:
                try:
                    # Procurar botão "carregar mais" ou "próxima" na seção PU
                    more_btns = driver.find_elements(By.XPATH,
                        "//button[contains(text(),'Carregar mais') or contains(text(),'Mais') "
                        "or contains(text(),'Próxim')]"
                    )
                    clicked = False
                    for btn in more_btns:
                        if btn.is_displayed():
                            safe_click(driver, btn)
                            time.sleep(1.5)
                            clicked = True
                            break

                    if not clicked:
                        # Scroll para baixo na tabela
                        driver.execute_script(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                        time.sleep(1)
                except Exception:
                    pass
            else:
                break

        except Exception as e:
            logger.debug("Erro ao extrair PU (tentativa %d): %s", scroll_attempt, e)
            break

    logger.debug("Extraidos %d registros de PU de %s", len(pu_data), url)
    return pu_data


def scrape_documents(driver, url: str) -> list[dict]:
    """Extrai links de documentos de todas as abas de documentos."""
    docs = []
    if not url:
        return docs

    driver.get(url)
    time.sleep(REQUEST_DELAY)
    dismiss_cookies(driver)
    close_chat_widget(driver)

    categorias = ["Documentos", "Relatórios Anuais", "Relatórios de Garantias", "Assembleias"]

    for categoria in categorias:
        try:
            # Clicar na aba da categoria
            tab_found = False
            for selector in [
                f"//button[contains(text(),'{categoria}')]",
                f"//a[contains(text(),'{categoria}')]",
                f"//span[contains(text(),'{categoria}')]",
                f"//li[contains(text(),'{categoria}')]",
                f"//div[contains(@role,'tab')][contains(text(),'{categoria}')]",
                f"//*[contains(@class,'tab')][contains(text(),'{categoria}')]",
            ]:
                tabs = driver.find_elements(By.XPATH, selector)
                for t in tabs:
                    if t.is_displayed():
                        safe_click(driver, t)
                        time.sleep(1.5)
                        tab_found = True
                        break
                if tab_found:
                    break

            if not tab_found:
                logger.debug("Aba '%s' nao encontrada em: %s", categoria, url)
                continue

            # Expandir seções colapsadas, se houver
            try:
                expandables = driver.find_elements(By.CSS_SELECTOR,
                    "[class*='expand'], [class*='collaps'], [class*='accordion'], "
                    "button[aria-expanded='false']"
                )
                for exp in expandables:
                    if exp.is_displayed():
                        safe_click(driver, exp)
                        time.sleep(0.5)
            except Exception:
                pass

            # Coletar links de PDF
            pdf_links = driver.find_elements(By.CSS_SELECTOR,
                "a[href$='.pdf'], a[href*='.pdf'], "
                "a[href*='download'], a[href*='documento'], "
                "a[href*='storage'], a[href*='blob']"
            )

            for link in pdf_links:
                try:
                    href = link.get_attribute("href")
                    nome = link.text.strip() or link.get_attribute("title") or ""
                    if not nome:
                        nome = link.get_attribute("download") or ""
                    if not nome:
                        # Extrair nome do URL
                        nome = href.split("/")[-1].split("?")[0] if href else "documento"

                    if href:
                        docs.append({
                            "categoria": categoria,
                            "nome": sanitize_filename(nome),
                            "url": href,
                        })
                except Exception:
                    continue

            # Se não encontrou via <a>, tentar botões de download
            if not any(d["categoria"] == categoria for d in docs):
                download_btns = driver.find_elements(By.CSS_SELECTOR,
                    "button[class*='download'], button[aria-label*='download'], "
                    "[class*='download'] button"
                )
                for btn in download_btns:
                    try:
                        nome = btn.text.strip() or btn.get_attribute("aria-label") or "documento"
                        onclick = btn.get_attribute("onclick") or ""
                        data_url = btn.get_attribute("data-url") or ""
                        url_doc = data_url or onclick
                        if url_doc:
                            docs.append({
                                "categoria": categoria,
                                "nome": sanitize_filename(nome),
                                "url": url_doc,
                            })
                    except Exception:
                        continue

            logger.debug("Categoria '%s': %d documentos encontrados.",
                        categoria, sum(1 for d in docs if d["categoria"] == categoria))

        except Exception as e:
            logger.warning("Erro ao processar aba '%s': %s", categoria, e)
            continue

    logger.debug("Total de %d documentos encontrados em %s", len(docs), url)
    return docs


# ─────────────────────────────────────────────────────────────────────
# Download de Documentos
# ─────────────────────────────────────────────────────────────────────
def download_pdf(url: str, dest_path: Path) -> bool:
    """Baixa um arquivo PDF usando requests."""
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if dest_path.exists():
            logger.debug("Arquivo já existe: %s", dest_path)
            return True

        headers = {"User-Agent": USER_AGENT}
        response = requests.get(url, headers=headers, timeout=60, stream=True)
        response.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.debug("Download concluído: %s", dest_path)
        return True

    except Exception as e:
        logger.warning("Erro ao baixar %s: %s", url, e)
        return False


def process_documents(conn: sqlite3.Connection, emissao_id: int,
                      identificador: str, docs: list[dict]):
    """Processa e baixa documentos de uma emissão."""
    if not docs:
        return

    safe_id = sanitize_filename(identificador)

    for doc in docs:
        categoria = doc["categoria"]
        nome = doc["nome"]
        url = doc["url"]

        # Garantir extensão .pdf
        if not nome.lower().endswith(".pdf"):
            nome += ".pdf"

        safe_cat = sanitize_filename(categoria)
        dest_path = DOCS_DIR / safe_id / safe_cat / nome

        # Registrar no banco
        upsert_documento(conn, emissao_id, categoria, nome, url, str(dest_path))

        # Baixar
        if download_pdf(url, dest_path):
            conn.execute(
                "UPDATE documentos SET baixado = 1 WHERE emissao_id = ? AND categoria = ? AND nome_arquivo = ?",
                (emissao_id, categoria, nome)
            )
            conn.commit()


# ─────────────────────────────────────────────────────────────────────
# Orquestração Principal
# ─────────────────────────────────────────────────────────────────────
def process_emission_details(driver, conn: sqlite3.Connection,
                             emissao_id: int, url_detalhe: str,
                             identificador: str, nome: str,
                             skip_docs: bool = False,
                             only_recent_pu: bool = False):
    """Processa os detalhes completos de uma emissão."""
    if not url_detalhe:
        logger.debug("Sem URL de detalhe para emissão ID=%d (%s).", emissao_id, nome)
        return

    # 1) Informações básicas da página de detalhes
    try:
        details = scrape_detail_page(driver, url_detalhe)

        # Apenas atualizar datas (campos seguros, sem UNIQUE constraint)
        updates = {}
        if details.get("data_emissao"):
            updates["data_emissao"] = details["data_emissao"]
        if details.get("data_vencimento"):
            updates["data_vencimento"] = details["data_vencimento"]

        # Atualizar codigo_isin somente se a emissao ainda nao tem um
        row = conn.execute(
            "SELECT codigo_isin FROM emissoes WHERE id = ?", (emissao_id,)
        ).fetchone()
        if row and not row[0] and details.get("codigo_isin"):
            updates["codigo_isin"] = details["codigo_isin"]

        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            params = list(updates.values()) + [datetime.now().isoformat(), emissao_id]
            try:
                conn.execute(
                    f"UPDATE emissoes SET {set_clause}, atualizado_em = ? WHERE id = ?",
                    params,
                )
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                # Tentar sem campos que causam conflito UNIQUE
                safe_updates = {k: v for k, v in updates.items()
                                if k in ("data_emissao", "data_vencimento")}
                if safe_updates:
                    sc = ", ".join(f"{k} = ?" for k in safe_updates)
                    conn.execute(
                        f"UPDATE emissoes SET {sc}, atualizado_em = ? WHERE id = ?",
                        list(safe_updates.values()) + [datetime.now().isoformat(), emissao_id],
                    )
                    conn.commit()
    except Exception as e:
        logger.warning("Erro ao extrair detalhes de %s: %s", nome, e)

    # 2) Histórico de PU
    try:
        pu_history = scrape_pu_history(driver, url_detalhe, only_recent=only_recent_pu)
        for pu in pu_history:
            upsert_pu(conn, emissao_id, pu["data"], pu["valor_pu"])
        conn.commit()
        if pu_history:
            logger.info("  PU: %d registros para %s", len(pu_history), nome)
    except Exception as e:
        logger.warning("Erro ao extrair PU de %s: %s", nome, e)

    # 3) Documentos
    if not skip_docs:
        try:
            docs = scrape_documents(driver, url_detalhe)
            process_documents(conn, emissao_id, identificador, docs)
            if docs:
                logger.info("  Docs: %d documentos para %s", len(docs), nome)
        except Exception as e:
            logger.warning("Erro ao processar documentos de %s: %s", nome, e)


def run_full_scrape(max_pages: Optional[int] = None, skip_docs: bool = False):
    """Execução completa: coleta listagem + detalhes + PU + documentos."""
    logger.info("=" * 60)
    logger.info("INICIO - Scrape completo Vortx DCM")
    logger.info("=" * 60)

    conn = init_database(DB_PATH)
    driver = create_driver()

    try:
        # Fase 1: Coleta da listagem
        logger.info("-- Fase 1: Coleta da listagem --")
        scrape_all_listings(driver, conn, max_pages=max_pages)

        # Fase 2: Detalhes de cada emissão
        logger.info("-- Fase 2: Detalhes das emissoes --")
        emissoes = get_all_emissoes(conn)
        total = len(emissoes)
        logger.info("Processando detalhes de %d emissoes.", total)

        for idx, (em_id, cod_if, cod_isin, url_det, nome, dt_venc) in enumerate(emissoes, 1):
            identificador = cod_if or cod_isin or f"emissao_{em_id}"

            if idx % 100 == 0:
                logger.info("Progresso detalhes: %d/%d (%.1f%%)", idx, total, idx / total * 100)

            try:
                process_emission_details(
                    driver, conn, em_id, url_det, identificador, nome or "",
                    skip_docs=skip_docs, only_recent_pu=False
                )
            except Exception as e:
                logger.error("Erro na emissao %d (%s): %s", em_id, nome, e)
                continue

            time.sleep(REQUEST_DELAY)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
    except Exception as e:
        logger.error("Erro fatal: %s", e, exc_info=True)
    finally:
        driver.quit()
        conn.close()
        logger.info("Scrape completo finalizado.")
        logger.info("=" * 60)


def run_pu_update():
    """Atualização diária: verifica PU dos últimos 5 dias para emissões não vencidas."""
    logger.info("=" * 60)
    logger.info("INICIO - Atualizacao diaria de PU")
    logger.info("=" * 60)

    conn = init_database(DB_PATH)
    driver = create_driver()

    try:
        # Primeiro atualizar listagem para capturar novas emissões
        logger.info("-- Fase 1: Atualizando listagem --")
        scrape_all_listings(driver, conn)

        # Depois atualizar PU apenas de emissões não vencidas
        logger.info("-- Fase 2: Atualizando PU (ultimos 5 dias) --")
        emissoes = get_emissoes_para_atualizar_pu(conn)
        total = len(emissoes)
        logger.info("Atualizando PU de %d emissoes nao vencidas.", total)

        for idx, (em_id, cod_if, cod_isin, url_det, nome) in enumerate(emissoes, 1):
            identificador = cod_if or cod_isin or f"emissao_{em_id}"

            if idx % 100 == 0:
                logger.info("Progresso PU: %d/%d (%.1f%%)", idx, total, idx / total * 100)

            try:
                if url_det:
                    pu_history = scrape_pu_history(driver, url_det, only_recent=True)
                    for pu in pu_history:
                        upsert_pu(conn, em_id, pu["data"], pu["valor_pu"])
                    conn.commit()
                    if pu_history:
                        logger.info("  PU atualizado: %d registros para %s",
                                    len(pu_history), nome)
            except Exception as e:
                logger.warning("Erro ao atualizar PU de %s: %s", nome, e)
                continue

            time.sleep(REQUEST_DELAY)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
    except Exception as e:
        logger.error("Erro fatal: %s", e, exc_info=True)
    finally:
        driver.quit()
        conn.close()
        logger.info("Atualizacao de PU finalizada.")
        logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────
def parse_args():
    """Parse argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Scraper Vortx DCM - Coleta de emissões de títulos de dívida."
    )
    parser.add_argument(
        "--update-pu", action="store_true",
        help="Apenas atualizar PU dos últimos 5 dias (emissões não vencidas)."
    )
    parser.add_argument(
        "--skip-docs", action="store_true",
        help="Pular download de documentos PDF."
    )
    parser.add_argument(
        "--max-pages", type=int, default=None,
        help="Limitar número de páginas de listagem (útil para testes)."
    )
    return parser.parse_args()


def main():
    """Ponto de entrada principal."""
    args = parse_args()

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Configuracoes:")
    logger.info("  DB: %s", DB_PATH)
    logger.info("  Documentos: %s", DOCS_DIR)
    logger.info("  Logs: %s", log_file)
    logger.info("  Modo: %s", "Atualizacao PU" if args.update_pu else "Scrape completo")

    if args.update_pu:
        run_pu_update()
    else:
        run_full_scrape(max_pages=args.max_pages, skip_docs=args.skip_docs)


if __name__ == "__main__":
    main()
