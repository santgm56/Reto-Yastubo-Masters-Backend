from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_user_key(user_id: str | None) -> str:
    normalized = (user_id or "").strip()
    return normalized if normalized else "customer-demo"


@dataclass
class _CustomerPortalState:
    beneficiaries: list[dict]
    beneficiary_sequence: int
    death_report: dict
    death_sequence: int
    payment_method: dict


class CustomerPortalService:
    _state_by_user: dict[str, _CustomerPortalState] = {}
    _lock = Lock()

    def _get_state(self, user_id: str | None) -> _CustomerPortalState:
        user_key = _normalize_user_key(user_id)

        with self._lock:
            state = self._state_by_user.get(user_key)
            if state:
                return state

            seeded = _CustomerPortalState(
                beneficiaries=[
                    {
                        "id": 1,
                        "nombre": "Titular Portal",
                        "documento": f"DOC-{user_key}",
                        "parentesco": "Titular",
                        "estado": "activo",
                    }
                ],
                beneficiary_sequence=1,
                death_report={
                    "payload": {
                        "nombreReportante": "",
                        "documentoReportante": "",
                        "nombreFallecido": "",
                        "documentoFallecido": "",
                        "fechaFallecimiento": "",
                        "observacion": "",
                        "canalContacto": "email",
                    },
                    "confirmation": {
                        "estadoCaso": "NO_RECONOCIDO",
                        "referenciaCaso": "",
                        "siguientePaso": "",
                        "fechaReporte": "",
                    },
                    "operationalState": "normal",
                    "context": [
                        {"key": "policy-context", "label": "Contexto poliza", "value": "Disponible"},
                        {"key": "support-channel", "label": "Canal soporte", "value": "Portal cliente"},
                    ],
                },
                death_sequence=0,
                payment_method={
                    "reference": "CARD-DEFAULT-001",
                    "brand": "VISA",
                    "masked": "**** **** **** 4242",
                    "status": "ACTIVE",
                    "updated_at": _now_iso(),
                },
            )
            self._state_by_user[user_key] = seeded
            return seeded

    def modules(self, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        pending_count = 1 if state.payment_method["status"] != "ACTIVE" else 0
        method_state = "Con alerta" if pending_count else "Estable"

        return {
            "dashboard": {
                "description": "Vision rapida de productos, pagos y alertas de operacion.",
                "currentState": "activo" if pending_count == 0 else "alerta_pago",
                "blockedReason": None,
                "allowedActions": [
                    {"label": "Ir a productos", "routeName": "customer.products"},
                    {"label": "Revisar pagos pendientes", "routeName": "customer.payments.pending"},
                ],
                "blocks": [
                    {"title": "Productos activos", "value": "2", "hint": "Resumen funcional del portal cliente."},
                    {"title": "Cuotas pendientes", "value": str(pending_count), "hint": "Derivado del estado de pago."},
                    {"title": "Estado metodo", "value": method_state, "hint": "Estado operativo del metodo principal."},
                    {"title": "Ultima actualizacion", "value": state.payment_method["updated_at"], "hint": "Sincronizado desde API."},
                ],
                "timeline": [
                    {"code": "EVT-101", "title": "Login customer", "detail": "Sesion iniciada correctamente en portal cliente."},
                ],
            },
            "productos": {
                "description": "Productos contratados con estado, vigencia y acciones disponibles.",
                "currentState": "activo",
                "blockedReason": None,
                "allowedActions": [
                    {"label": "Ver transacciones", "routeName": "customer.transactions"},
                    {"label": "Solicitar anulacion", "routeName": "customer.products"},
                ],
                "blocks": [
                    {"title": "Plan principal", "value": "Vigente", "hint": "Cobertura activa y visible en portal."},
                    {"title": "Plan complementario", "value": "Revision", "hint": "Flujo de detalle disponible para cliente."},
                    {"title": "Metodo pago", "value": method_state, "hint": "Estado operativo del cobro."},
                    {"title": "Accion sugerida", "value": "Revisar terminos", "hint": "Validar coberturas y beneficiarios."},
                ],
                "timeline": [
                    {"code": "EVT-205", "title": "Producto emitido", "detail": "Emision confirmada y visible en portal."},
                ],
            },
            "transacciones": {
                "description": "Historial financiero con resultado de intentos de cobro y conciliacion.",
                "currentState": "reconciliado" if pending_count == 0 else "con_alerta",
                "blockedReason": None,
                "allowedActions": [
                    {"label": "Exportar historial", "routeName": None},
                    {"label": "Ir a metodo de pago", "routeName": "customer.payment-method"},
                ],
                "blocks": [
                    {"title": "Transacciones mes", "value": "3", "hint": "Registros visibles por cliente autenticado."},
                    {"title": "Monto total", "value": "USD 142.00", "hint": "Suma aproximada de montos reportados."},
                    {"title": "Ultimo estado", "value": "PAID" if pending_count == 0 else "PAST_DUE", "hint": "Estado mas reciente del timeline de pago."},
                    {"title": "Webhook", "value": "synchronized", "hint": "Sincronizacion de eventos de pago."},
                ],
                "timeline": [
                    {"code": "EVT-302", "title": "Linea de pagos", "detail": "Cargada desde API customer/payment-history."},
                ],
            },
            "pagos-pendientes": {
                "description": "Cuotas por pagar, fecha limite y acciones para regularizar estado.",
                "currentState": "al_dia" if pending_count == 0 else "bloqueado_por_metodo",
                "blockedReason": None if pending_count == 0 else "Existe una cuota con riesgo o fallo. Actualiza metodo y reintenta cobro.",
                "allowedActions": [
                    {"label": "Actualizar metodo de pago", "routeName": "customer.payment-method"},
                    {"label": "Reintentar cobro", "simulateKey": "retry-payment"},
                ],
                "blocks": [
                    {"title": "Cuotas pendientes", "value": str(pending_count), "hint": "Calculado desde timeline de pagos."},
                    {"title": "Monto pendiente", "value": "USD 42.00" if pending_count else "USD 0.00", "hint": "Estimado del saldo con estado no conciliado."},
                    {"title": "Fecha limite", "value": datetime.now(UTC).strftime("%Y-%m-01"), "hint": "Fecha de referencia del ultimo ciclo."},
                    {"title": "Canal de cobro", "value": "Stripe", "hint": "Metodo de pago reportado por backend."},
                ],
                "timeline": [
                    {"code": "EVT-401", "title": "Cuota generada", "detail": "Obligacion de pago identificada para ciclo actual."},
                ],
            },
            "metodo-pago": {
                "description": "Gestion de tarjeta principal y acciones de actualizacion/eliminacion.",
                "currentState": "metodo_actualizado" if pending_count == 0 else "requiere_actualizacion",
                "blockedReason": None if pending_count == 0 else "Metodo con riesgo detectado por historial reciente.",
                "allowedActions": [
                    {"label": "Actualizar tarjeta", "simulateKey": "update-payment-method"},
                    {"label": "Volver a pagos pendientes", "routeName": "customer.payments.pending"},
                ],
                "blocks": [
                    {"title": "Metodo principal", "value": state.payment_method["masked"], "hint": "Flujo funcional de consulta y actualizacion."},
                    {"title": "Estado metodo", "value": method_state, "hint": "Evaluado por resultado de pagos."},
                    {"title": "Ultima actualizacion", "value": state.payment_method["updated_at"], "hint": "Referencia operativa para soporte."},
                    {"title": "Siguiente accion", "value": "Actualizar" if pending_count else "Sin accion requerida", "hint": "Recomendacion de portal."},
                ],
                "timeline": [
                    {"code": "EVT-511", "title": "Metodo disponible", "detail": "Metodo asociado para cobro recurrente."},
                ],
            },
        }

    def beneficiaries_index(self, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        items = list(state.beneficiaries)
        has_blocked = any(item.get("estado") == "bloqueado" for item in items)
        has_alert = any(item.get("estado") == "incompleto" for item in items)

        return {
            "items": items,
            "total": len(items),
            "operationalState": "bloqueado" if has_blocked else "alerta" if has_alert else "normal",
            "lastUpdate": _now_iso(),
        }

    def beneficiaries_store(self, payload: dict, user_id: str | None) -> tuple[dict | None, dict | None]:
        state = self._get_state(user_id)
        document = str(payload.get("documento") or "").strip().lower()

        if any(str(item.get("documento") or "").strip().lower() == document for item in state.beneficiaries):
            return None, {
                "code": "API_VALIDATION_ERROR",
                "message": "El beneficiario ya existe para este cliente.",
                "errors": {
                    "documento": ["El documento ya esta registrado en la lista de beneficiarios."],
                },
                "details": {
                    "field": "documento",
                },
            }

        state.beneficiary_sequence += 1
        item = {
            "id": state.beneficiary_sequence,
            "nombre": str(payload.get("nombre") or "").strip(),
            "documento": str(payload.get("documento") or "").strip(),
            "parentesco": str(payload.get("parentesco") or "").strip(),
            "estado": str(payload.get("estado") or "activo").strip().lower(),
        }

        state.beneficiaries.insert(0, item)
        return item, None

    def death_report_show(self, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        return state.death_report

    def death_report_store(self, payload: dict, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        state.death_sequence += 1
        reference = f"FALL-{datetime.now(UTC).strftime('%Y%m%d')}-{state.death_sequence:03d}"

        state.death_report = {
            "payload": {
                "nombreReportante": str(payload.get("nombreReportante") or "").strip(),
                "documentoReportante": str(payload.get("documentoReportante") or "").strip(),
                "nombreFallecido": str(payload.get("nombreFallecido") or "").strip(),
                "documentoFallecido": str(payload.get("documentoFallecido") or "").strip(),
                "fechaFallecimiento": str(payload.get("fechaFallecimiento") or "").strip(),
                "observacion": str(payload.get("observacion") or "").strip(),
                "canalContacto": str(payload.get("canalContacto") or "email").strip().lower(),
            },
            "confirmation": {
                "estadoCaso": "RECIBIDO",
                "referenciaCaso": reference,
                "siguientePaso": "Nuestro equipo validara la informacion y te contactara por el canal registrado.",
                "fechaReporte": _now_iso(),
            },
            "operationalState": "normal",
            "context": state.death_report.get("context", []),
        }
        return state.death_report

    def payment_method_show(self, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        return {
            "payment_method": state.payment_method,
        }

    def payment_method_upsert(self, payload: dict, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        method_ref = str(payload.get("reference") or "").strip()
        method_brand = str(payload.get("brand") or "CARD").strip().upper()
        if len(method_ref) >= 4:
            masked = f"**** **** **** {method_ref[-4:]}"
        else:
            masked = "**** **** **** 0000"

        state.payment_method = {
            "reference": method_ref,
            "brand": method_brand,
            "masked": masked,
            "status": "ACTIVE",
            "updated_at": _now_iso(),
        }
        return {
            "payment_method": state.payment_method,
        }

    def payment_method_delete(self, user_id: str | None) -> dict:
        state = self._get_state(user_id)
        state.payment_method = {
            "reference": "",
            "brand": "",
            "masked": "Sin metodo",
            "status": "REMOVED",
            "updated_at": _now_iso(),
        }
        return {
            "payment_method": state.payment_method,
        }