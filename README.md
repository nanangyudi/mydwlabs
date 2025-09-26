# mydwlabs — Odoo + Mage + Postgres + DuckDB + ClickHouse (+ CloudBeaver)

## Prasyarat
- Docker & Docker Compose v2

## Setup
```bash
cp .env.example .env
# edit .env (password, port, filter DB, dsb)
mkdir -p clickhouse/{data,logs} cloudbeaver/workspace

## MENJALANKAN
docker compose up -d
# (opsional) UI CloudBeaver
docker compose -f docker-compose.cloudbeaver.yml up -d

## AKSES
Odoo: http://localhost:${ODOO_PORT}
Mage: http://localhost:${MAGE_PORT}
ClickHouse HTTP: http://localhost:${CH_HTTP_PORT}
(Opsional) CloudBeaver: http://localhost:${CLOUDBEAVER_PORT}

## CATATAN
* Jangan commit .env, file DuckDB, filestore Odoo, data ClickHouse (sudah di .gitignore).
* Koneksi Mage → Postgres/ClickHouse disimpan di ./mage/mage_data/* (jangan commit).
