# FastAPI Backend (Migracion Real)

Backend Python/FastAPI que replica los contratos operativos de la implementacion Laravel:

- `POST /api/v1/issuances/quote`
- `POST /api/v1/issuances`
- `GET /api/v1/issuances/{contract_id}`
- `GET /api/v1/issuances/{contract_id}/pdf`
- `POST /api/v1/issuances/{contract_id}/send-email`
- `GET /api/v1/issuances`
- `GET /api/v1/payments`
- `POST /api/v1/payments/checkout`
- `POST /api/v1/payments/subscribe`
- `POST /api/v1/payments/{monthly_record_id}/checkout`
- `POST /api/v1/payments/{monthly_record_id}/subscribe`
- `POST /api/v1/payments/{monthly_record_id}/retry`
- `POST /api/v1/payments/webhook`
- `POST /api/v1/payments/webhooks/stripe`
- `GET /api/v1/cancellations`
- `POST /api/v1/cancellations`
- `GET /api/customer/payments`
- `GET /api/customer/payment-history`
- `GET /api/customer/payments/status`
- `GET /api/customer/portal/modules`
- `GET/POST /api/customer/beneficiaries`
- `GET/POST /api/customer/death-report`
- `GET/POST/DELETE /api/customer/payment-method`
- `GET /api/v1/admin/audit`
- `GET /health`

## Contrato de error unificado

Todos los errores HTTP y de validacion retornan contrato estandar consumible por frontend:

```json
{
	"code": "API_VALIDATION_ERROR",
	"message": "La solicitud no cumple las reglas de validacion.",
	"errors": {
		"campo": ["mensaje"]
	},
	"details": {
		"origin": "request_validation"
	},
	"request_id": "req_xxx"
}
```

Esto garantiza paridad con adaptadores FE para `code/message/details/request_id` y errores por campo.

## Variables de entorno

Crear archivo `.env` en `backend-yastubo/`:

```env
APP_NAME=GFA Emisiones API (FastAPI)
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=
DB_DATABASE=gfadb
CORS_ORIGINS=http://localhost:3000,http://localhost:5173,http://localhost:8000
```

## Ejecucion local

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001
```

Swagger: `http://localhost:8001/docs`

## Corrida reproducible Stripe sandbox (demo)

Objetivo: cubrir checklist de demo para pago exitoso/fallido y webhook idempotente en entorno de pruebas.

Prerequisitos:

- Backend arriba en `http://127.0.0.1:8001`.
- Cuenta Stripe en modo test.
- Stripe CLI instalado y autenticado (`stripe login`).

### 1) Iniciar listener local de webhook

```bash
stripe listen --forward-to http://127.0.0.1:8001/api/v1/payments/webhooks/stripe
```

Guardar el `event_id` recibido en consola para evidencia.

### 2) Simular webhook exitoso

```bash
curl -X POST "http://127.0.0.1:8001/api/v1/payments/webhooks/stripe" \
	-H "Content-Type: application/json" \
	-d '{"monthly_record_id":3201,"outcome":"success","event_id":"evt_demo_success_001"}'
```

Esperado:

- `ok: true`
- `data.status: PAID`
- `data.idempotent: false`

### 3) Simular webhook duplicado (idempotencia)

```bash
curl -X POST "http://127.0.0.1:8001/api/v1/payments/webhooks/stripe" \
	-H "Content-Type: application/json" \
	-d '{"monthly_record_id":3201,"outcome":"success","event_id":"evt_demo_success_001"}'
```

Esperado:

- `ok: true`
- `message: "Webhook duplicado ignorado"`
- `data.idempotent: true`

### 4) Simular webhook fallido controlado

```bash
curl -X POST "http://127.0.0.1:8001/api/v1/payments/webhooks/stripe" \
	-H "Content-Type: application/json" \
	-d '{"monthly_record_id":3201,"outcome":"failed","event_id":"evt_demo_failed_001"}'
```

Esperado:

- `ok: true`
- `data.status: FAILED`
- `data.idempotent: false`

### 5) Evidencia a anexar en cierre v2.0

- Captura de respuesta JSON de exito.
- Captura de respuesta JSON de fallo.
- Captura de respuesta JSON duplicada con `idempotent=true`.
- Log/trace del listener de Stripe CLI.

Automatizacion opcional en Windows PowerShell:

```powershell
./scripts/run_stripe_demo.ps1 -BaseUrl "http://127.0.0.1:8001" -MonthlyRecordId 3201
```
