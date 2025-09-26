-- Buat DB hanya jika belum ada (aman di-re-run)
SELECT 'CREATE DATABASE odoo   OWNER odoo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'odoo') \gexec;

SELECT 'CREATE DATABASE mageai OWNER odoo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mageai') \gexec;

-- Ekstensi yang berguna untuk Odoo & pipeline (idempotent)
\connect odoo
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

\connect mageai
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

