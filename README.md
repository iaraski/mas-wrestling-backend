# Backend (FastAPI)

## Env

- Пример переменных: [.env.example](file:///c:/Users/marin/projects/CompEaseBot/backend/.env.example)
- Обязательные:
  - `DATABASE_URL` (Postgres, `postgresql+asyncpg://...`)
  - `MINIO_ENDPOINT`, `MINIO_BUCKET`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`

## Local auth

Для логина/паролей используется таблица `public.auth_passwords`.
SQL для создания: [local_auth.sql](file:///c:/Users/marin/projects/CompEaseBot/backend/sql/local_auth.sql)
