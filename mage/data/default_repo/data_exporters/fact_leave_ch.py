#from mage_ai.data_preparation.decorators import data_exporter
import clickhouse_connect
import pandas as pd

@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    if df is None or df.empty:
        print("[export] DF kosong, skip"); return

    # Bersihkan timezone: ClickHouse DateTime (tanpa tz) → kirim naive UTC
    for c in ['request_date_from','request_date_to','create_date','write_date','_ingested_at']:
        if c in df.columns:
            s = pd.to_datetime(df[c], utc=True, errors='coerce')
            df[c] = s.dt.tz_convert('UTC').dt.tz_localize(None)

    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='odoo', password='odooadm', database='odoo_dwh'
    )

    client.query("""
        CREATE TABLE IF NOT EXISTS fact_leave
        (
          leave_id          UInt64,
          employee_id       UInt64,
          leave_type_id     Nullable(UInt64),     -- <- buat Nullable kalau bisa NULL
          request_date_from Nullable(DateTime),
          request_date_to   Nullable(DateTime),
          number_of_days    Nullable(Float32),
          state             LowCardinality(String),
          create_date       Nullable(DateTime),
          write_date        DateTime,
          _ingested_at      DateTime
        )
        ENGINE = ReplacingMergeTree(write_date)
        ORDER BY (leave_id)
    """)

    # Ini yang penting: gunakan insert_df, bukan insert(list-of-dicts)
    client.insert_df('fact_leave', df)
    print(f"[export] inserted rows: {len(df)}")
