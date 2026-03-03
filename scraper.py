"""
Scraper Vortx DCM - Coleta de Emissoes de Titulos de Divida (v2 - API)
========================================================================
Usa a API REST da Vortx (apis.vortx.com.br) para coletar dados de emissoes
(CRA, CRI, Debentures, LF, etc.), incluindo historico completo de PU e
download de documentos PDF via Selenium.

Uso:
    python scraper.py                  # Execucao completa
    python scraper.py --update-pu      # Apenas atualizar PU (ultimos 5 dias)
    python scraper.py --skip-docs      # Pular download de documentos
    python scraper.py --skip-pu        # Pular coleta de PU
    python scraper.py --max-pages 5    # Limitar a N paginas de listagem (teste)
    python scraper.py --max-ops 50     # Limitar a N operacoes nos detalhes (teste)
"""

import os
import re
import sys
import math
import time
import sqlite3
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# ---------------------------------------------------------------------------
# Configuracoes
# ---------------------------------------------------------------------------
BASE_API = "https://apis.vortx.com.br/vxsite/api"
BASE_SITE = "https://www.vortx.com.br/investidor/dcm"
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "vortx_dcm.db"
DOCS_DIR = SCRIPT_DIR / "documentos"
LOG_DIR = SCRIPT_DIR / "logs"

REQUEST_DELAY = 0.3          # Delay entre requisicoes de listagem (segundos)
DETAIL_DELAY = 0.15          # Delay entre chamadas de detalhe
PU_DELAY = 0.15              # Delay entre chamadas de PU
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
API_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.vortx.com.br",
    "Referer": "https://www.vortx.com.br/",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Banco de Dados
# ---------------------------------------------------------------------------
def init_database(db_path: Path) -> sqlite3.Connection:
    """Inicializa o banco SQLite com as tabelas necessarias."""
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Criar tabelas (sem indices em colunas que podem nao existir em DBs v1)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS emissoes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            vortx_id            INTEGER UNIQUE,
            codigo_if           TEXT,
            codigo_isin         TEXT,
            tipo                TEXT,
            tipo_descricao      TEXT,
            nome                TEXT,
            emissor             TEXT,
            cnpj_emissor        TEXT,
            numero_emissao      TEXT,
            serie               TEXT,
            trustee             TEXT,
            volume_emitido      REAL,
            quantidade_emitida  INTEGER,
            pu_emissao          REAL,
            data_emissao        TEXT,
            data_vencimento     TEXT,
            remuneracao         TEXT,
            indexador            TEXT,
            taxa_juros           REAL,
            amortizacao          TEXT,
            pagamento_juros      TEXT,
            distribuicao         TEXT,
            tipo_risco           TEXT,
            status               TEXT,
            url_detalhe          TEXT,
            atualizado_em        TEXT
        );

        CREATE TABLE IF NOT EXISTS historico_pu (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            emissao_id      INTEGER NOT NULL,
            vortx_id        INTEGER,
            data            TEXT NOT NULL,
            valor_pu        REAL,
            valor_pu_vazio  REAL,
            juros           REAL,
            amortizacao     REAL,
            total           REAL,
            valor_juros     REAL,
            valor_nominal   REAL,
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
    """)

    # Migrar tabela existente se veio da v1 (antes de criar indices)
    _migrate_database(conn)

    # Criar indices (apos migracao garantir que as colunas existem)
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_emissoes_vortx_id
            ON emissoes(vortx_id);
        CREATE INDEX IF NOT EXISTS idx_emissoes_codigo_if
            ON emissoes(codigo_if);
        CREATE INDEX IF NOT EXISTS idx_emissoes_codigo_isin
            ON emissoes(codigo_isin);
        CREATE INDEX IF NOT EXISTS idx_emissoes_vencimento
            ON emissoes(data_vencimento);
        CREATE INDEX IF NOT EXISTS idx_pu_emissao_data
            ON historico_pu(emissao_id, data);
        CREATE INDEX IF NOT EXISTS idx_pu_vortx_id
            ON historico_pu(vortx_id);
    """)

    conn.commit()
    logger.info("Banco de dados inicializado: %s", db_path)
    return conn


def _migrate_database(conn: sqlite3.Connection):
    """Adiciona colunas novas a tabelas existentes (migracao da v1)."""
    existing_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(emissoes)").fetchall()
    }

    new_cols = {
        "vortx_id": "INTEGER",
        "tipo_descricao": "TEXT",
        "quantidade_emitida": "INTEGER",
        "pu_emissao": "REAL",
        "remuneracao": "TEXT",
        "indexador": "TEXT",
        "taxa_juros": "REAL",
        "amortizacao": "TEXT",
        "pagamento_juros": "TEXT",
        "distribuicao": "TEXT",
        "tipo_risco": "TEXT",
        "status": "TEXT",
    }

    for col, col_type in new_cols.items():
        if col not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE emissoes ADD COLUMN {col} {col_type}")
                logger.info("Coluna '%s' adicionada a tabela emissoes.", col)
            except sqlite3.OperationalError:
                pass

    # Criar indice UNIQUE em vortx_id se ainda nao existe
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_emissoes_vortx_id_unique
            ON emissoes(vortx_id)
        """)
    except sqlite3.OperationalError:
        pass

    # Migrar historico_pu
    pu_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(historico_pu)").fetchall()
    }
    pu_new_cols = {
        "vortx_id": "INTEGER",
        "valor_pu_vazio": "REAL",
        "juros": "REAL",
        "amortizacao": "REAL",
        "total": "REAL",
        "valor_juros": "REAL",
        "valor_nominal": "REAL",
    }
    for col, col_type in pu_new_cols.items():
        if col not in pu_cols:
            try:
                conn.execute(f"ALTER TABLE historico_pu ADD COLUMN {col} {col_type}")
                logger.info("Coluna '%s' adicionada a tabela historico_pu.", col)
            except sqlite3.OperationalError:
                pass


def upsert_emissao(conn: sqlite3.Connection, data: dict) -> int:
    """Insere ou atualiza uma emissao pelo vortx_id. Retorna o ID interno."""
    now = datetime.now().isoformat()
    data["atualizado_em"] = now

    conn.execute("""
        INSERT INTO emissoes (
            vortx_id, codigo_if, codigo_isin, tipo, tipo_descricao, nome,
            emissor, cnpj_emissor, numero_emissao, serie, trustee,
            volume_emitido, quantidade_emitida, pu_emissao,
            data_emissao, data_vencimento, remuneracao, indexador, taxa_juros,
            amortizacao, pagamento_juros, distribuicao, tipo_risco, status,
            url_detalhe, atualizado_em
        ) VALUES (
            :vortx_id, :codigo_if, :codigo_isin, :tipo, :tipo_descricao, :nome,
            :emissor, :cnpj_emissor, :numero_emissao, :serie, :trustee,
            :volume_emitido, :quantidade_emitida, :pu_emissao,
            :data_emissao, :data_vencimento, :remuneracao, :indexador, :taxa_juros,
            :amortizacao, :pagamento_juros, :distribuicao, :tipo_risco, :status,
            :url_detalhe, :atualizado_em
        )
        ON CONFLICT(vortx_id)
        DO UPDATE SET
            codigo_if       = COALESCE(excluded.codigo_if, emissoes.codigo_if),
            codigo_isin     = COALESCE(excluded.codigo_isin, emissoes.codigo_isin),
            tipo            = COALESCE(excluded.tipo, emissoes.tipo),
            tipo_descricao  = COALESCE(excluded.tipo_descricao, emissoes.tipo_descricao),
            nome            = COALESCE(excluded.nome, emissoes.nome),
            emissor         = COALESCE(excluded.emissor, emissoes.emissor),
            cnpj_emissor    = COALESCE(excluded.cnpj_emissor, emissoes.cnpj_emissor),
            numero_emissao  = COALESCE(excluded.numero_emissao, emissoes.numero_emissao),
            serie           = COALESCE(excluded.serie, emissoes.serie),
            trustee         = COALESCE(excluded.trustee, emissoes.trustee),
            volume_emitido  = COALESCE(excluded.volume_emitido, emissoes.volume_emitido),
            quantidade_emitida = COALESCE(excluded.quantidade_emitida, emissoes.quantidade_emitida),
            pu_emissao      = COALESCE(excluded.pu_emissao, emissoes.pu_emissao),
            data_emissao    = COALESCE(excluded.data_emissao, emissoes.data_emissao),
            data_vencimento = COALESCE(excluded.data_vencimento, emissoes.data_vencimento),
            remuneracao     = COALESCE(excluded.remuneracao, emissoes.remuneracao),
            indexador       = COALESCE(excluded.indexador, emissoes.indexador),
            taxa_juros      = COALESCE(excluded.taxa_juros, emissoes.taxa_juros),
            amortizacao     = COALESCE(excluded.amortizacao, emissoes.amortizacao),
            pagamento_juros = COALESCE(excluded.pagamento_juros, emissoes.pagamento_juros),
            distribuicao    = COALESCE(excluded.distribuicao, emissoes.distribuicao),
            tipo_risco      = COALESCE(excluded.tipo_risco, emissoes.tipo_risco),
            status          = COALESCE(excluded.status, emissoes.status),
            url_detalhe     = COALESCE(excluded.url_detalhe, emissoes.url_detalhe),
            atualizado_em   = excluded.atualizado_em
    """, data)
    conn.commit()

    row = conn.execute(
        "SELECT id FROM emissoes WHERE vortx_id = ?", (data["vortx_id"],)
    ).fetchone()
    return row[0] if row else 0


def upsert_pu(conn: sqlite3.Connection, emissao_id: int, vortx_id: int, record: dict):
    """Insere ou atualiza um registro de PU."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO historico_pu (
            emissao_id, vortx_id, data, valor_pu, valor_pu_vazio,
            juros, amortizacao, total, valor_juros, valor_nominal, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(emissao_id, data)
        DO UPDATE SET
            valor_pu = excluded.valor_pu,
            valor_pu_vazio = excluded.valor_pu_vazio,
            juros = excluded.juros,
            amortizacao = excluded.amortizacao,
            total = excluded.total,
            valor_juros = excluded.valor_juros,
            valor_nominal = excluded.valor_nominal,
            atualizado_em = excluded.atualizado_em
    """, (
        emissao_id, vortx_id,
        record["data"], record["valor_pu"], record.get("valor_pu_vazio"),
        record.get("juros"), record.get("amortizacao"), record.get("total"),
        record.get("valor_juros"), record.get("valor_nominal"), now,
    ))


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


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------
def create_session() -> requests.Session:
    """Cria sessao HTTP com retry e headers padrao."""
    session = requests.Session()
    session.headers.update(HEADERS)

    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry_strategy = Retry(
        total=RETRY_ATTEMPTS,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def api_get(session: requests.Session, endpoint: str, params: dict = None) -> dict:
    """Faz GET na API e retorna JSON."""
    url = f"{BASE_API}/{endpoint}"
    resp = session.get(url, params=params, timeout=API_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_api_date(date_str: Optional[str]) -> Optional[str]:
    """Converte data da API (ISO) para formato YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        return date_str[:10]
    except Exception:
        return None


def sanitize_filename(name: str) -> str:
    """Remove caracteres invalidos de nomes de arquivo."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:200]


# ---------------------------------------------------------------------------
# Fase 1: Coleta da Listagem via API
# ---------------------------------------------------------------------------
def fetch_all_operations(session: requests.Session,
                         max_pages: Optional[int] = None) -> list[dict]:
    """Coleta todas as operacoes da listagem via API paginada."""
    data = api_get(session, "operacoes", {"pagina": 1})
    total = data["data"]["total"]
    total_pages = math.ceil(total / 10)

    if max_pages:
        total_pages = min(total_pages, max_pages)

    logger.info("Total de operacoes na Vortx: %d (%d paginas). Coletando %d paginas...",
                total, math.ceil(total / 10), total_pages)

    all_ops = list(data["data"]["operations"])
    failed_pages = []

    for page in range(2, total_pages + 1):
        try:
            data = api_get(session, "operacoes", {"pagina": page})
            ops = data["data"]["operations"]
            all_ops.extend(ops)

            if page % 100 == 0:
                logger.info("  Listagem: pagina %d/%d (%d operacoes coletadas)",
                            page, total_pages, len(all_ops))
        except Exception as e:
            logger.warning("Erro na pagina %d: %s", page, e)
            failed_pages.append(page)

        time.sleep(REQUEST_DELAY)

    # Retry paginas que falharam
    if failed_pages:
        logger.info("Retentando %d paginas que falharam...", len(failed_pages))
        time.sleep(RETRY_DELAY)
        for page in failed_pages:
            try:
                data = api_get(session, "operacoes", {"pagina": page})
                all_ops.extend(data["data"]["operations"])
            except Exception as e:
                logger.error("Pagina %d falhou novamente: %s", page, e)
            time.sleep(REQUEST_DELAY * 2)

    logger.info("Listagem completa: %d operacoes coletadas.", len(all_ops))
    return all_ops


def save_listing_to_db(conn: sqlite3.Connection,
                       operations: list[dict]) -> dict:
    """Salva as operacoes da listagem no banco. Retorna {vortx_id: emissao_id}."""
    vortx_to_db = {}
    saved = 0
    errors = 0

    # Mapear operation_type_id para descricao
    tipo_map = {
        "1": "CRI", "2": "CRA", "3": "DEB", "4": "LF",
        "5": "LCI", "6": "LCA", "7": "FIDC",
    }

    for op in operations:
        try:
            vortx_id = int(op.get("codigo", 0))
            if not vortx_id:
                errors += 1
                continue

            op_type_id = str(op.get("operation_type_id", ""))
            tipo = tipo_map.get(op_type_id, op_type_id)

            data = {
                "vortx_id": vortx_id,
                "codigo_if": op.get("codIf", "") or None,
                "codigo_isin": None,
                "tipo": tipo or None,
                "tipo_descricao": None,
                "nome": op.get("apelido", "") or None,
                "emissor": op.get("emissor", "") or None,
                "cnpj_emissor": op.get("cnpj_emissora", "") or None,
                "numero_emissao": op.get("emissao", "") or None,
                "serie": op.get("serie", "") or None,
                "trustee": op.get("agFiduciario", "") or None,
                "volume_emitido": None,
                "quantidade_emitida": None,
                "pu_emissao": None,
                "data_emissao": None,
                "data_vencimento": None,
                "remuneracao": None,
                "indexador": None,
                "taxa_juros": None,
                "amortizacao": None,
                "pagamento_juros": None,
                "distribuicao": None,
                "tipo_risco": None,
                "status": None,
                "url_detalhe": f"{BASE_SITE}/operacao?id={vortx_id}",
            }

            em_id = upsert_emissao(conn, data)
            vortx_to_db[vortx_id] = em_id
            saved += 1

        except Exception as e:
            logger.warning("Erro ao salvar operacao %s: %s", op.get("codigo"), e)
            errors += 1

    logger.info("Listagem salva: %d operacoes, %d erros.", saved, errors)
    return vortx_to_db


# ---------------------------------------------------------------------------
# Fase 2: Detalhes de cada Operacao via API
# ---------------------------------------------------------------------------
def fetch_operation_details(session: requests.Session, vortx_id: int) -> Optional[dict]:
    """Busca detalhes completos de uma operacao."""
    try:
        data = api_get(session, f"operacao/{vortx_id}")
        ops = data.get("operations", [])
        if not ops:
            return None
        return ops[0]
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.debug("Operacao %d nao encontrada (404).", vortx_id)
        else:
            logger.warning("Erro HTTP operacao %d: %s", vortx_id, e)
        return None
    except Exception as e:
        logger.warning("Erro operacao %d: %s", vortx_id, e)
        return None


def parse_operation_details(detail: dict, vortx_id: int) -> dict:
    """Converte detalhes da API para formato do banco."""
    op = detail.get("operation", {})
    op_type = detail.get("operationType", {})
    risk = detail.get("risk", {})
    part = detail.get("part", {})

    return {
        "vortx_id": vortx_id,
        "codigo_if": op.get("ifCode", "") or None,
        "codigo_isin": op.get("isinCode") or None,
        "tipo": op_type.get("description", "") or None,
        "tipo_descricao": op_type.get("name", "") or None,
        "nome": op.get("nickname", "") or None,
        "emissor": part.get("partName", "") or None,
        "cnpj_emissor": op.get("issuerId", "") or part.get("partCnpj", "") or None,
        "numero_emissao": str(op.get("issue", "")) if op.get("issue") is not None else None,
        "serie": str(op.get("serie", "")) if op.get("serie") is not None else None,
        "trustee": None,  # Preservado da listagem
        "volume_emitido": op.get("totalVolume"),
        "quantidade_emitida": op.get("quantity"),
        "pu_emissao": op.get("nominalUnitValue"),
        "data_emissao": parse_api_date(op.get("issueDate")),
        "data_vencimento": parse_api_date(op.get("issueDuty")),
        "remuneracao": op.get("remuneration", "") or None,
        "indexador": op.get("indexerDescription", "") or None,
        "taxa_juros": op.get("interestRate"),
        "amortizacao": op.get("periodoAmortizacao", "") or None,
        "pagamento_juros": op.get("periodoPagamentoJuros", "") or None,
        "distribuicao": op.get("schemePlacmentDescriptinon", "") or None,
        "tipo_risco": risk.get("description", "") or None,
        "status": op.get("defaultPeriod", "") or None,
        "url_detalhe": f"{BASE_SITE}/operacao?id={vortx_id}",
    }


def update_operation_details(conn: sqlite3.Connection,
                             session: requests.Session,
                             vortx_ids: list[int],
                             vortx_to_db: dict):
    """Atualiza detalhes de todas as operacoes via API."""
    total = len(vortx_ids)
    logger.info("Buscando detalhes de %d operacoes...", total)

    updated = 0
    errors = 0
    not_found = 0

    for idx, vortx_id in enumerate(vortx_ids, 1):
        try:
            detail = fetch_operation_details(session, vortx_id)
            if not detail:
                not_found += 1
                continue

            parsed = parse_operation_details(detail, vortx_id)

            # Preservar trustee preenchido na listagem
            row = conn.execute(
                "SELECT trustee FROM emissoes WHERE vortx_id = ?", (vortx_id,)
            ).fetchone()
            if row and row[0]:
                parsed["trustee"] = row[0]

            upsert_emissao(conn, parsed)
            updated += 1

        except Exception as e:
            logger.warning("Erro detalhe operacao %d: %s", vortx_id, e)
            errors += 1

        if idx % 200 == 0:
            logger.info("  Detalhes: %d/%d (%.1f%%) - ok=%d, 404=%d, erros=%d",
                        idx, total, idx / total * 100, updated, not_found, errors)

        time.sleep(DETAIL_DELAY)

    logger.info("Detalhes concluido: %d atualizados, %d nao encontrados, %d erros.",
                updated, not_found, errors)


# ---------------------------------------------------------------------------
# Fase 3: Historico de PU via API
# ---------------------------------------------------------------------------
def fetch_pu_history(session: requests.Session, vortx_id: int) -> list[dict]:
    """Busca historico COMPLETO de PU (uma unica chamada retorna TODOS)."""
    try:
        data = api_get(session, f"operacao/{vortx_id}/preco-unitario/historico-pagamentos")
        if not data.get("success"):
            return []

        records = []
        for item in data.get("unitPrices", []):
            payment_date = parse_api_date(item.get("paymentDate"))
            if not payment_date:
                continue

            records.append({
                "data": payment_date,
                "valor_pu": item.get("unitPriceFull"),
                "valor_pu_vazio": item.get("unitPriceEmpty"),
                "juros": item.get("interest"),
                "amortizacao": item.get("amortization"),
                "total": item.get("total"),
                "valor_juros": item.get("interestValue"),
                "valor_nominal": item.get("nominalValue"),
            })

        return records

    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            pass
        else:
            logger.warning("Erro HTTP PU operacao %d: %s", vortx_id, e)
        return []
    except Exception as e:
        logger.warning("Erro PU operacao %d: %s", vortx_id, e)
        return []


def update_all_pu(conn: sqlite3.Connection,
                  session: requests.Session,
                  vortx_ids: list[int],
                  vortx_to_db: dict,
                  only_recent: bool = False):
    """Atualiza historico de PU para todas as operacoes."""
    total = len(vortx_ids)
    logger.info("Buscando PU de %d operacoes%s...",
                total, " (ultimos 5 dias)" if only_recent else " (completo)")

    cutoff_date = None
    if only_recent:
        cutoff_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    total_pu = 0
    ops_with_pu = 0
    errors = 0

    for idx, vortx_id in enumerate(vortx_ids, 1):
        try:
            emissao_id = vortx_to_db.get(vortx_id)
            if not emissao_id:
                row = conn.execute(
                    "SELECT id FROM emissoes WHERE vortx_id = ?", (vortx_id,)
                ).fetchone()
                if row:
                    emissao_id = row[0]
                else:
                    continue

            records = fetch_pu_history(session, vortx_id)
            if not records:
                continue

            if cutoff_date:
                records = [r for r in records if r["data"] >= cutoff_date]

            if records:
                for record in records:
                    upsert_pu(conn, emissao_id, vortx_id, record)
                conn.commit()

                total_pu += len(records)
                ops_with_pu += 1

        except Exception as e:
            logger.warning("Erro PU operacao %d: %s", vortx_id, e)
            errors += 1

        if idx % 200 == 0:
            logger.info("  PU: %d/%d (%.1f%%) - %d registros de %d operacoes, %d erros",
                        idx, total, idx / total * 100, total_pu, ops_with_pu, errors)

        time.sleep(PU_DELAY)

    logger.info("PU concluido: %d registros de %d operacoes, %d erros.",
                total_pu, ops_with_pu, errors)


# ---------------------------------------------------------------------------
# Fase 4: Documentos via Selenium (nao ha API)
# ---------------------------------------------------------------------------
def create_driver():
    """Cria Chrome WebDriver para download de documentos."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions

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

    prefs = {
        "download.default_directory": str(DOCS_DIR),
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver


def scrape_documents(driver, url: str) -> list[dict]:
    """Extrai links de documentos via Selenium."""
    from selenium.webdriver.common.by import By

    docs = []
    if not url:
        return docs

    try:
        driver.get(url)
    except Exception:
        time.sleep(3)
        try:
            driver.get(url)
        except Exception:
            return docs

    time.sleep(2)

    categorias = ["Documentos", "Relatorios Anuais", "Relatorios de Garantias", "Assembleias"]

    for categoria in categorias:
        try:
            tabs = driver.find_elements(By.XPATH,
                f"//button[contains(text(),'{categoria}')] | "
                f"//a[contains(text(),'{categoria}')] | "
                f"//span[contains(text(),'{categoria}')]"
            )
            tab_clicked = False
            for t in tabs:
                if t.is_displayed():
                    try:
                        t.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", t)
                    time.sleep(1.5)
                    tab_clicked = True
                    break

            if not tab_clicked:
                continue

            pdf_links = driver.find_elements(By.CSS_SELECTOR,
                "a[href$='.pdf'], a[href*='.pdf'], a[href*='download'], "
                "a[href*='storage'], a[href*='blob']"
            )

            for link in pdf_links:
                try:
                    href = link.get_attribute("href")
                    nome = link.text.strip() or link.get_attribute("title") or ""
                    if not nome:
                        nome = link.get_attribute("download") or ""
                    if not nome and href:
                        nome = href.split("/")[-1].split("?")[0]

                    if href:
                        docs.append({
                            "categoria": categoria,
                            "nome": sanitize_filename(nome) if nome else "documento",
                            "url": href,
                        })
                except Exception:
                    continue

        except Exception as e:
            logger.debug("Erro aba '%s': %s", categoria, e)

    return docs


def download_pdf(session: requests.Session, url: str, dest_path: Path) -> bool:
    """Baixa um arquivo PDF."""
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.exists():
            return True

        resp = session.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        logger.debug("Erro download %s: %s", url, e)
        return False


# ---------------------------------------------------------------------------
# Orquestracao Principal
# ---------------------------------------------------------------------------
def run_full_scrape(max_pages: Optional[int] = None,
                    max_ops: Optional[int] = None,
                    skip_docs: bool = False,
                    skip_pu: bool = False):
    """Execucao completa: listagem + detalhes + PU + documentos."""
    logger.info("=" * 60)
    logger.info("INICIO - Scrape completo Vortx DCM (v2 - API)")
    logger.info("=" * 60)

    conn = init_database(DB_PATH)
    session = create_session()

    try:
        # Fase 1: Listagem
        logger.info("-- Fase 1: Coleta da listagem via API --")
        operations = fetch_all_operations(session, max_pages=max_pages)
        vortx_to_db = save_listing_to_db(conn, operations)

        all_vortx_ids = sorted(vortx_to_db.keys())
        if max_ops:
            all_vortx_ids = all_vortx_ids[:max_ops]
            logger.info("Limitado a %d operacoes.", max_ops)

        # Fase 2: Detalhes
        logger.info("-- Fase 2: Detalhes via API --")
        update_operation_details(conn, session, all_vortx_ids, vortx_to_db)

        # Fase 3: PU
        if not skip_pu:
            logger.info("-- Fase 3: Historico de PU via API --")
            update_all_pu(conn, session, all_vortx_ids, vortx_to_db, only_recent=False)
        else:
            logger.info("-- Fase 3: PU pulado (--skip-pu) --")

        # Fase 4: Documentos
        if not skip_docs:
            logger.info("-- Fase 4: Documentos via Selenium --")
            driver = None
            try:
                driver = create_driver()
                emissoes = conn.execute("""
                    SELECT id, vortx_id, codigo_if, codigo_isin, nome, url_detalhe
                    FROM emissoes
                    WHERE url_detalhe IS NOT NULL AND url_detalhe != ''
                    ORDER BY vortx_id
                """).fetchall()

                total_docs = 0
                for idx, (em_id, v_id, cod_if, cod_isin, nome, url) in enumerate(emissoes, 1):
                    if max_ops and idx > max_ops:
                        break
                    if idx % 200 == 0:
                        logger.info("  Docs: %d/%d (%.1f%%)",
                                    idx, len(emissoes), idx / len(emissoes) * 100)
                    try:
                        docs = scrape_documents(driver, url)
                        if docs:
                            identificador = sanitize_filename(
                                cod_if or cod_isin or nome or f"op_{v_id}"
                            )
                            for doc in docs:
                                nome_arq = doc["nome"]
                                if not nome_arq.lower().endswith(".pdf"):
                                    nome_arq += ".pdf"
                                cat = sanitize_filename(doc["categoria"])
                                dest = DOCS_DIR / identificador / cat / nome_arq

                                upsert_documento(conn, em_id, doc["categoria"],
                                                 nome_arq, doc["url"], str(dest))

                                if download_pdf(session, doc["url"], dest):
                                    conn.execute(
                                        "UPDATE documentos SET baixado = 1 "
                                        "WHERE emissao_id = ? AND categoria = ? AND nome_arquivo = ?",
                                        (em_id, doc["categoria"], nome_arq)
                                    )
                            conn.commit()
                            total_docs += len(docs)
                    except Exception as e:
                        logger.warning("Erro docs operacao %d: %s", v_id, e)

                    time.sleep(1.5)

                logger.info("Documentos concluido: %d documentos.", total_docs)
            finally:
                if driver:
                    driver.quit()
        else:
            logger.info("-- Fase 4: Documentos pulados (--skip-docs) --")

        _print_stats(conn)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuario.")
    except Exception as e:
        logger.error("Erro fatal: %s", e, exc_info=True)
    finally:
        conn.close()
        logger.info("Scrape completo finalizado.")
        logger.info("=" * 60)


def run_pu_update():
    """Atualizacao diaria: PU dos ultimos 5 dias para emissoes nao vencidas."""
    logger.info("=" * 60)
    logger.info("INICIO - Atualizacao diaria de PU (v2 - API)")
    logger.info("=" * 60)

    conn = init_database(DB_PATH)
    session = create_session()

    try:
        # Atualizar listagem
        logger.info("-- Fase 1: Atualizando listagem --")
        operations = fetch_all_operations(session)
        vortx_to_db = save_listing_to_db(conn, operations)

        # Detalhes para emissoes sem data
        logger.info("-- Fase 2: Detalhes de emissoes sem dados --")
        new_ops = conn.execute("""
            SELECT vortx_id FROM emissoes
            WHERE data_emissao IS NULL AND vortx_id IS NOT NULL
        """).fetchall()
        if new_ops:
            new_ids = [row[0] for row in new_ops]
            logger.info("  %d emissoes sem detalhes.", len(new_ids))
            update_operation_details(conn, session, new_ids, vortx_to_db)

        # PU de emissoes nao vencidas
        logger.info("-- Fase 3: Atualizando PU (ultimos 5 dias) --")
        hoje = datetime.now().strftime("%Y-%m-%d")
        emissoes = conn.execute("""
            SELECT vortx_id FROM emissoes
            WHERE vortx_id IS NOT NULL
              AND (data_vencimento IS NULL OR data_vencimento = '' OR data_vencimento >= ?)
        """, (hoje,)).fetchall()

        vortx_ids = [row[0] for row in emissoes]
        logger.info("  %d emissoes nao vencidas.", len(vortx_ids))
        update_all_pu(conn, session, vortx_ids, vortx_to_db, only_recent=True)

        _print_stats(conn)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuario.")
    except Exception as e:
        logger.error("Erro fatal: %s", e, exc_info=True)
    finally:
        conn.close()
        logger.info("Atualizacao de PU finalizada.")
        logger.info("=" * 60)


def _print_stats(conn: sqlite3.Connection):
    """Imprime estatisticas do banco."""
    stats = {
        "emissoes_total": conn.execute("SELECT COUNT(*) FROM emissoes").fetchone()[0],
        "com_vortx_id": conn.execute("SELECT COUNT(*) FROM emissoes WHERE vortx_id IS NOT NULL").fetchone()[0],
        "com_isin": conn.execute("SELECT COUNT(*) FROM emissoes WHERE codigo_isin IS NOT NULL AND codigo_isin != ''").fetchone()[0],
        "com_data_emissao": conn.execute("SELECT COUNT(*) FROM emissoes WHERE data_emissao IS NOT NULL").fetchone()[0],
        "com_data_vencimento": conn.execute("SELECT COUNT(*) FROM emissoes WHERE data_vencimento IS NOT NULL").fetchone()[0],
        "registros_pu": conn.execute("SELECT COUNT(*) FROM historico_pu").fetchone()[0],
        "ops_com_pu": conn.execute("SELECT COUNT(DISTINCT emissao_id) FROM historico_pu").fetchone()[0],
        "documentos": conn.execute("SELECT COUNT(*) FROM documentos").fetchone()[0],
        "docs_baixados": conn.execute("SELECT COUNT(*) FROM documentos WHERE baixado = 1").fetchone()[0],
    }

    logger.info("-- Estatisticas do banco --")
    for key, val in stats.items():
        logger.info("  %-25s %s", key, f"{val:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Scraper Vortx DCM v2 - Coleta via API REST"
    )
    parser.add_argument("--update-pu", action="store_true",
                        help="Apenas atualizar PU (ultimos 5 dias, emissoes nao vencidas).")
    parser.add_argument("--skip-docs", action="store_true",
                        help="Pular download de documentos PDF.")
    parser.add_argument("--skip-pu", action="store_true",
                        help="Pular coleta de historico de PU.")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Limitar paginas de listagem (10 ops/pagina).")
    parser.add_argument("--max-ops", type=int, default=None,
                        help="Limitar operacoes para detalhes/PU/docs.")
    return parser.parse_args()


def main():
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
        run_full_scrape(
            max_pages=args.max_pages,
            max_ops=args.max_ops,
            skip_docs=args.skip_docs,
            skip_pu=args.skip_pu,
        )


if __name__ == "__main__":
    main()
