# Runbook: Recuperacion OAuth y Continuidad

## Sintoma

Facturador deja de procesar y en Cloud Run aparecen errores:

- `google.auth.exceptions.RefreshError`
- `invalid_grant`
- `Token has been expired or revoked`

## Causa

El refresh token de OAuth (Gmail/Drive) fue revocado o expiro.

Nota importante:
- si el OAuth consent screen esta en `Testing` y el proyecto es `External`, Google puede emitir refresh tokens con vencimiento de 7 dias para scopes de Gmail/Drive;
- mover el almacenamiento del token ayuda a la continuidad operativa, pero no elimina por si solo ese vencimiento.

## Recuperacion inmediata

1. Ejecuta la rotacion asistida:
   - `powershell -ExecutionPolicy Bypass -File .\rotate_google_oauth_token.ps1 -ProjectId <PROJECT_ID>`
   - Si el refresh token ya no sirve, completa el consentimiento en navegador con la misma cuenta Gmail del negocio.
2. Verifica en logs:
   - sin nuevos `invalid_grant`
   - jobs Scheduler exitosos en siguientes ciclos.
   - token actualizado en Firestore si `FACTURADOR_TOKEN_STATE_COLLECTION` esta configurado.

## Recuperacion manual (fallback)

1. Regenera token local:
   - `python run_mail_automation.py --once --verbose`
   - Completa el consentimiento en navegador con la misma cuenta Gmail del negocio.
2. Verifica que se actualizo `config/google_token.json`.
3. Sube nueva version del secreto:
   - `gcloud secrets versions add facturador-google-token --data-file config/google_token.json --project <PROJECT_ID>`
4. Fuerza nueva revision de Cloud Run para recargar secretos y corregir el topic de Gmail:
   - `gcloud run services update facturador-gmail-trigger --region us-central1 --project <PROJECT_ID> --update-env-vars FACTURADOR_WATCH_TOPIC=projects/<OAUTH_PROJECT_ID>/topics/facturador-gmail-updates,FACTURADOR_TOKEN_ROTATION_TS=<timestamp>`
5. Ejecuta recuperacion operativa:
   - `gcloud scheduler jobs run facturador-watch-renew --location us-central1 --project <PROJECT_ID>`
   - `gcloud scheduler jobs run facturador-full-sync --location us-central1 --project <PROJECT_ID>`

## Endpoints esperados (modo degradado OAuth)

- `GET /healthz`: `200`, `{"ok": true, "automation_ready": false, "reason": "oauth_invalid_grant"}`
- `POST /admin/start-watch`: `503`, payload con `code=oauth_unavailable`
- `POST /admin/full-sync`: `503`, payload con `code=oauth_unavailable`
- `POST /pubsub/push`: `200`, payload con `degraded=true`

## Prevencion recomendada

1. Publica OAuth Consent Screen en **Production**.
2. Configura `FACTURADOR_TOKEN_STATE_COLLECTION` y `FACTURADOR_TOKEN_STATE_DOC` para persistir el refresh token en Firestore.
3. Mantener monitoreo de:
   - errores `invalid_grant`
   - fallos de Scheduler (`watch-renew`, `full-sync`)
   - salud degradada (`/healthz` con `automation_ready=false`).
