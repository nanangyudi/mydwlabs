import duckdb, pandas as pd
from pandas import DataFrame
from pathlib import Path
#from mage_ai.data_preparation.decorators import data_exporter
from mage_ai.data_preparation.variable_manager import get_global_variables

def upsert_to_duckdb(df: pd.DataFrame, path: str, table: str, key: str, order_col: str='write_date'):
    if df is None or df.empty:
        print("[export] DF kosong, skip")
        return

    con = duckdb.connect(path)
    cols = list(df.columns)
    col_list  = ', '.join(cols)
    val_list  = ', '.join([f"n.{c}" for c in cols])
    set_clause = ', '.join([f"{c}=n.{c}" for c in cols])

    try:
        con.register('newdata', df)
        # buat tabel target jika belum ada (skema dari DF)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM newdata WHERE 1=0;")

        try:
            # coba pakai MERGE (butuh duckdb >= 1.4)
            con.execute(f"""
                MERGE INTO {table} AS t
                USING newdata AS n
                ON t.{key} = n.{key}
                WHEN MATCHED AND coalesce(n.{order_col}, TIMESTAMP '1970-01-01')
                               >= coalesce(t.{order_col}, TIMESTAMP '1970-01-01')
                  THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({col_list}) VALUES ({val_list});
            """)
            print("[export] MERGE ok")
        except Exception as e:
            # Fallback universal: DELETE + INSERT (tanpa MERGE)
            print(f"[export] MERGE gagal ({e}); fallback DELETE+INSERT")
            con.execute("BEGIN")
            con.execute("CREATE TEMP TABLE stg AS SELECT * FROM newdata;")
            # hapus baris existing utk key yang ada di batch ini
            con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM stg);")
            # insert semua dari batch (hasilnya = versi terbaru dari batch)
            con.execute(f"INSERT INTO {table} SELECT * FROM stg;")
            con.execute("COMMIT")
            print("[export] Fallback ok")
    finally:
        try:
            con.unregister('newdata')
        except Exception:
            pass
        con.close()
        
@data_exporter
def export_data(df: DataFrame, *args, **kwargs) -> None:
    duckdb_path = get_global_variables(kwargs['pipeline_uuid']).get('duckdb_path', '/home/src/duckdb/odoo_wh.duckdb')
    Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"[export] rows in df: {0 if df is None else len(df)}")
    upsert_to_duckdb(df, duckdb_path, table='fact_leave', key='leave_id', order_col='write_date')