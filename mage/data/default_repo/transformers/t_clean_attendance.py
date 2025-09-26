import pandas as pd

@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    df = df.copy()
    for c in ['check_in','check_out','create_date','write_date']:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
    df['_ingested_at'] = pd.Timestamp.now(tz='UTC')
    return df
