# Example: reliable WhatsApp webhooks from Evolution API

Evolution API posts a JSON body like this for every event (`messages.upsert`,
`connection.update`, etc.):

```json
{
  "event": "messages.upsert",
  "instance": "my-instance",
  "data": { "key": { "id": "...", "remoteJid": "..." }, "message": { "conversation": "hi" } },
  "server_url": "https://your-evolution-api.example.com",
  "apikey": "your-instance-token",
  "date_time": "2026-08-26T12:00:00.000Z"
}
```

This example shows the pattern for making that reliable:

1. Configure the instance's webhook with a custom `Authorization: Bearer <secret>`
   header (Evolution API's webhook settings support custom headers per instance).
2. `main.py` verifies that header, enqueues the event into Postgres via
   `hookrelay`, and returns immediately.
3. A background `Worker` processes events, retrying with backoff on failure and
   moving anything that exhausts its retries to the dead-letter queue.
4. `data.key.id` (the WhatsApp message id) is used as the idempotency key, so a
   webhook Evolution API resends after a timeout isn't processed twice.

## Run it

```bash
pip install "hookrelay[fastapi,postgres]" uvicorn
export WEBHOOK_SHARED_SECRET=change-me
export DATABASE_URL=postgresql+asyncpg://user:pass@localhost/hookrelay
uvicorn main:app --reload
```

Point your Evolution API instance's webhook at
`http://your-host:8000/webhooks/whatsapp` with the header above, and inspect
anything that ends up dead-lettered with:

```python
await backend.list_dead_letters()
```
