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

## Testing

### Load Sources Configuration

```powershell
Invoke-RestMethod -Uri "http://localhost:7071/api/mgmt/sources" -Method GET | ConvertTo-Json -Depth 5
```

### Trigger Ingestion (Manual)

```powershell
# Ingest and flush immediately (force_flush=true)
$response = Invoke-RestMethod -Uri "http://localhost:7071/api/ingest/werkportal/api/invoice?force_flush=true" -Method POST
Write-Host "Orchestration ID: $($response.id)"
Start-Sleep -Seconds 15
Invoke-RestMethod -Uri $response.statusQueryGetUri -Method GET | Select-Object runtimeStatus, output | ConvertTo-Json -Depth 10
```

```powershell
# Ingest and buffer (no force_flush, waits for min_rows or max_age)
$response = Invoke-RestMethod -Uri "http://localhost:7071/api/ingest/werkportal/api/invoice" -Method POST
Write-Host "Orchestration ID: $($response.id)"
Start-Sleep -Seconds 20
Invoke-RestMethod -Uri $response.statusQueryGetUri -Method GET | Select-Object runtimeStatus, output | ConvertTo-Json -Depth 10
```

### List Generated Parquet Files (Azure)

```powershell
az storage blob list --account-name synplaygrounddatalake --container-name stage-fs \
  --prefix "werkportal/api/invoice/delta" --auth-mode login --output table
```

### Inspect Parquet File

```powershell
# Download and analyze
$file = "werkportal/api/invoice/delta/2026-01-29T19-06-39Z.parquet"
az storage blob download --account-name synplaygrounddatalake --container-name stage-fs \
  --name $file --file "C:\temp\test.parquet" --auth-mode login --output none

# Check row count and columns
python -c "
import pyarrow.parquet as pq
t = pq.read_table(r'C:\temp\test.parquet')
print(f'Rows: {t.num_rows}')
print(f'Columns: {t.column_names}')
print(f'Unique invoice_ids: {t.to_pandas()[\"object_id\"].nunique()}')
"
```

### Update Configuration in Azurite (Local Testing)

> **Important:** Die Function App lädt die Config aus dem Blob Storage (Azurite), nicht aus der lokalen Datei! Nach jeder Änderung an `sources.json` muss diese hochgeladen werden.

```powershell
# Connection String für Azurite (muss in jeder neuen Terminal-Session gesetzt werden)
$conn = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

# Upload sources.json
az storage blob upload --container-name config --name sources.json --file "config\sources.json" --connection-string $conn --overwrite

# Upload schema (optional, nur bei Änderungen)
az storage blob upload --container-name config --name werkportal-invoice.json --file "config\schemas\werkportal-invoice.json" --connection-string $conn --overwrite \
  --connection-string $conn --overwrite
```


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
