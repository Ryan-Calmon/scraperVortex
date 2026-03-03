# Scraper Vortx DCM (v2 - API)

Este projeto é um **Scraper** desenvolvido em Python para coletar dados de emissões de títulos de dívida (CRA, CRI, Debêntures, LF, etc.) diretamente da plataforma **Vortx DCM**. Ele utiliza a API REST da Vortx para dados estruturados e Selenium para a extração de documentos PDF.

O objetivo principal é manter um banco de dados SQLite local atualizado com informações detalhadas das operações, histórico completo de Preço Unitário (PU) e cópias locais dos documentos oficiais.

---

## 🚀 Funcionalidades

| Funcionalidade | Descrição | Método |
| :--- | :--- | :--- |
| **Listagem de Operações** | Coleta a lista completa de ativos disponíveis na plataforma. | API REST |
| **Detalhes da Emissão** | Extrai dados técnicos como ISIN, taxas, indexadores e prazos. | API REST |
| **Histórico de PU** | Obtém o histórico completo de pagamentos e variações de PU. | API REST |
| **Download de Documentos** | Identifica e baixa PDFs (Atas, Prospectos, Relatórios). | Selenium |
| **Persistência** | Armazenamento estruturado em banco de dados SQLite. | SQL |
| **Logs Detalhados** | Rastreamento de execução e erros em arquivos e console. | Logging |

---

## 🛠️ Tecnologias Utilizadas

*   **Python 3.x**: Linguagem base.
*   **Requests**: Para consumo eficiente da API REST.
*   **Selenium**: Para automação de navegador e extração de documentos.
*   **SQLite3**: Banco de dados relacional leve e integrado.
*   **Logging**: Sistema de monitoramento de execução.
*   **Argparse**: Interface de linha de comando flexível.

---

## 📋 Pré-requisitos

Antes de executar o projeto, certifique-se de ter instalado:

1.  **Python 3.8+**
2.  **Google Chrome** (ou Chromium)
3.  **ChromeDriver** compatível com sua versão do Chrome

Instale as dependências via `pip`:

```bash
pip install requests selenium
```

---

## 📖 Como Usar

O script oferece diferentes modos de execução via linha de comando:

### Execução Completa
Realiza todas as fases (Listagem, Detalhes, PU e Documentos):
```bash
python scraper.py
```

### Atualização Diária (Apenas PU)
Ideal para rodar em agendadores (Cron/Task Scheduler). Atualiza apenas os últimos 5 dias de PU para emissões não vencidas:
```bash
python scraper.py --update-pu
```

### Opções de Filtro e Teste
*   **Pular documentos:** `python scraper.py --skip-docs`
*   **Pular histórico de PU:** `python scraper.py --skip-pu`
*   **Limitar páginas de listagem:** `python scraper.py --max-pages 5`
*   **Limitar número de operações:** `python scraper.py --max-ops 50`

---

## 📂 Estrutura do Projeto

*   `scraper.py`: Script principal contendo toda a lógica de coleta.
*   `vortx_dcm.db`: Banco de dados SQLite gerado automaticamente.
*   `documentos/`: Pasta onde os PDFs baixados são organizados por ativo e categoria.
*   `logs/`: Histórico de execução do scraper.

---

## 💡 Sugestões de Melhorias

Para elevar a qualidade e a robustez do código para um nível de produção, aqui estão algumas recomendações técnicas:


### 1. Melhoria na Performance
*   **Concorrência/Assincronia:** O uso de `asyncio` com `httpx` ou `aiohttp` para as chamadas de API poderia reduzir drasticamente o tempo de coleta, já que muitas requisições são feitas de forma sequencial.
*   **Multithreading:** O download de documentos via Selenium/Requests poderia ser paralelizado.

### 2. Robustez do Selenium
*   Implementar **Explicit Waits** em vez de `time.sleep()`. Isso torna o scraper mais rápido e menos propenso a falhas em conexões lentas.
*   Utilizar o modo `headless` de forma mais dinâmica ou integrar com serviços de proxy caso a plataforma implemente bloqueios.

### 3. Validação de Dados
*   Utilizar bibliotecas como **Pydantic** para validar os schemas de resposta da API, garantindo que mudanças silenciosas no backend da Vortx não quebrem o banco de dados.

## 4. Verificação dos dados
*   Não consegui testar o script por completo, portanto precisa saber se está sendo puxado todos os dados da base completos, mas o teste com escopo menor funcionou perfeitamente
