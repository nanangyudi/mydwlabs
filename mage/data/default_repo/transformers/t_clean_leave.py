#from mage_ai.data_preparation.decorators import transformer
import pandas as pd

@transformer
def transform(df, *args, **kwargs):
    df = df.copy()
    for c in ('request_date_from', 'request_date_to', 'create_date', 'write_date'):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
    df['_ingested_at'] = pd.Timestamp.now(tz='UTC')
    return df
