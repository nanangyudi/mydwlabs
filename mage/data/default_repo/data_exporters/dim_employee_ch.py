#from mage_ai.data_preparation.decorators import data_exporter
import clickhouse_connect
import pandas as pd

def _to_naive_utc(series):
    s = pd.to_datetime(series, utc=True, errors='coerce')
    return s.dt.tz_localize(None)

@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    if df is None or df.empty:
        print("[export dim_employee] DF kosong, skip"); return

    # --- Normalisasi kolom waktu ---
    for c in ['create_date', 'write_date', '_ingested_at']:
        if c in df.columns:
            df[c] = _to_naive_utc(df[c])

    # birthday -> Date (bukan DateTime)
    if 'birthday' in df.columns:
        s = pd.to_datetime(df['birthday'], utc=True, errors='coerce')
        df['birthday'] = s.dt.tz_localize(None).dt.date

    # active -> UInt8 (1/0/NULL)
    if 'active' in df.columns:
        b = df['active'].astype('boolean')
        df['active'] = b.apply(lambda x: 1 if x is True else (0 if x is False else None))

    # Pastikan kolom string ada (kalau tidak, buat NULL)
    for col in ['company_name','department_name','job_name','work_email','work_phone','mobile_phone','gender','employee_name']:
        if col not in df.columns:
            df[col] = None

    # Isi write_date jika kosong (pakai _ingested_at)  <-- perbaikan: tanpa spasi nyasar
    if 'write_date' in df.columns and '_ingested_at' in df.columns:
        df['write_date'] = df['write_date'].fillna(df['_ingested_at'])

    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='odoo', password='odooadm', database='odoo_dwh'
    )

    # DDL: gunakan LowCardinality(Nullable(String)), BUKAN Nullable(LowCardinality(String))
    client.query("""
        CREATE TABLE IF NOT EXISTS dim_employee
        (
          employee_id        UInt64,
          employee_name      LowCardinality(Nullable(String)),
          work_email         LowCardinality(Nullable(String)),
          work_phone         Nullable(String),
          mobile_phone       Nullable(String),
          gender             LowCardinality(Nullable(String)),
          birthday           Nullable(Date),
          active             Nullable(UInt8),
          company_id         Nullable(UInt64),
          company_name       LowCardinality(Nullable(String)),
          department_id      Nullable(UInt64),
          department_name    LowCardinality(Nullable(String)),
          job_id             Nullable(UInt64),
          job_name           LowCardinality(Nullable(String)),
          create_date        Nullable(DateTime),
          write_date         DateTime,
          _ingested_at       DateTime
        )
        ENGINE = ReplacingMergeTree(write_date)
        ORDER BY (employee_id)
    """)

    client.insert_df('dim_employee', df)
    print(f"[export dim_employee] inserted rows: {len(df)}")
