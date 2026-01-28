# Parquet Ingestion Function

Azure Durable Function App (Python) für die Erstellung von Parquet-Dateien aus konfigurierbaren API-Quellen mit intelligentem Buffering.

## Features

- **Stateful** - Durable Functions mit Event Sourcing für zuverlässigen State
- **Parametrierbar** - JSON-Config im Storage Account oder Environment Variables
- **Schema-basiert** - JSONPath für Payload-Extraktion (z.B. `data.rows[]`)
- **Intelligentes Buffering** - min_rows, max_rows, max_age für optimale Parquet-Größen
- **Multi-Source** - Timer, HTTP, Event Grid Trigger

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Triggers (Timer/HTTP/EventGrid)                                │
│              ↓                                                  │
│  Durable Orchestrator (Stateful)                               │
│              ↓                                                  │
│  Activities: LoadConfig → FetchAPI → Parse → Transform → Write │
│              ↓                                                  │
│  ADLS Gen2: /<concept>/<source>/<entity>/delta/*.parquet       │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Azure Functions Core Tools v4
- Azure Storage Account
- ADLS Gen2 Account

### Local Development

```bash
# Clone and setup
git clone https://github.com/fellnerd/parquet-ingestion-func.git
cd parquet-ingestion-func

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Copy local settings
cp local.settings.json.example local.settings.json
# Edit local.settings.json with your connection strings

# Run locally
func start
```

### Configuration

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for detailed configuration options.

#### Environment Variables

```bash
PARQUET_CONFIG_STORAGE_CONNECTION=<storage-connection-string>
PARQUET_CONFIG_CONTAINER=config
PARQUET_OUTPUT_ADLS_ACCOUNT=<adls-account-name>
PARQUET_OUTPUT_CONTAINER=stage-fs
```

#### Source Configuration (config/sources.json)

```json
{
  "sources": [
    {
      "id": "my-api",
      "concept": "mydata",
      "source": "api",
      "entity": "items",
      "fetch": {
        "endpoint": "${MY_API_ENDPOINT}",
        "auth": { "type": "bearer", "token": "${MY_API_TOKEN}" }
      },
      "response": {
        "data_path": "data.rows",
        "schema_ref": "schemas/my-items.json"
      },
      "buffer": { "min_rows": 100, "max_rows": 10000 }
    }
  ]
}
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ingest/{source_id}` | POST | Manual ingest with JSON body |
| `/api/admin/status` | GET | All buffer states |
| `/api/admin/flush/{source_id}` | POST | Force flush buffer |

## Project Structure

```
parquet-ingestion-func/
├── function_app.py          # Main entry point
├── host.json                # Function App config
├── requirements.txt         # Python dependencies
├── config/                  # Default configs
│   ├── sources.json.example
│   └── schemas/
├── orchestrators/           # Durable Orchestrators
├── activities/              # Durable Activities
├── triggers/                # Trigger Functions
├── core/                    # Shared utilities
├── tests/                   # Unit tests
└── infra/                   # Bicep templates
```

## Deployment

```bash
# Deploy via Azure CLI
az functionapp deployment source config-zip \
  -g rg-datavault-weu-001 \
  -n func-parquet-ingestion-weu-001 \
  --src deploy.zip
```

Or use the GitHub Actions workflow in `.github/workflows/deploy.yml`.

## License

MIT
