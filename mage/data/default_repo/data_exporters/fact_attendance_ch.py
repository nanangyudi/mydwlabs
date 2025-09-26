#from mage_ai.data_preparation.decorators import data_exporter
import clickhouse_connect
import pandas as pd

def _to_naive_utc(series):
    s = pd.to_datetime(series, utc=True, errors='coerce')
    return s.dt.tz_localize(None)

@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    if df is None or df.empty:
        print("[export fact_attendance] DF kosong, skip"); return

    # --- Normalisasi waktu ke naive UTC ---
    for c in ['check_in','check_out','create_date','write_date','_ingested_at']:
        if c in df.columns:
            df[c] = _to_naive_utc(df[c])

    # worked_hours bisa float / NaN
    if 'worked_hours' in df.columns:
        df['worked_hours'] = pd.to_numeric(df['worked_hours'], errors='coerce')

    # --- Koneksi ClickHouse ---
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123,
        username='odoo', password='odooadm', database='odoo_dwh'
    )

    # --- Buat tabel jika belum ada ---
    client.query("""
        CREATE TABLE IF NOT EXISTS fact_attendance
        (
          attendance_id   UInt64,
          employee_id     UInt64,
          check_in        Nullable(DateTime),
          check_out       Nullable(DateTime),
          worked_hours    Nullable(Float32),
          create_date     Nullable(DateTime),
          write_date      DateTime,
          _ingested_at    DateTime
        )
        ENGINE = ReplacingMergeTree(write_date)
        ORDER BY (attendance_id)
    """)

    # Isi write_date jika kosong
    if 'write_date' in df.columns and '_ingested_at' in df.columns:
        df['write_date'] = df['write_date'].fillna(df['_ingested_at'])

    # --- Insert ---
    client.insert_df('fact_attendance', df)
    print(f"[export fact_attendance] inserted rows: {len(df)}")
