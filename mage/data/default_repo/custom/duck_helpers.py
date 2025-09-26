import duckdb
import pandas as pd

def upsert_to_duckdb(df: pd.DataFrame, path: str, table: str, key: str, order_col: str = 'write_date'):
    if df is None or df.empty:
        return  # no-op

    con = duckdb.connect(path)
    try:
        df = df.copy()
        # daftarkan DF sebagai view sementara
        con.register('newdata', df)

        # buat tabel kalau belum ada (skema diambil otomatis dari DF)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM newdata WHERE 1=0;")

        cols = [c for c in df.columns]
        set_clause = ', '.join([f"{c}=n.{c}" for c in cols])
        col_list  = ', '.join(cols)
        val_list  = ', '.join([f"n.{c}" for c in cols])

        # update hanya jika versi data baru >= lama (berdasar order_col)
        on_clause = f"t.{key} = n.{key}"
        matched_cond = f"AND coalesce(n.{order_col}, TIMESTAMP '1970-01-01') >= coalesce(t.{order_col}, TIMESTAMP '1970-01-01')"

        con.execute(f"""
            MERGE INTO {table} t
            USING newdata n
            ON {on_clause}
            WHEN MATCHED {matched_cond} THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
        """)
        con.unregister('newdata')
    finally:
        con.close()
