import pandas as pd

@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    df = df.copy()
    # trim string
    for c in ['employee_name','work_email','work_phone','mobile_phone','gender',
              'department_name','job_name','company_name']:
        if c in df.columns:
            df[c] = df[c].astype('string').str.strip()
    # parse waktu → UTC
    for c in ['birthday','create_date','write_date']:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
    if 'active' in df.columns:
        df['active'] = df['active'].astype('boolean')
    # kolom housekeeping
    df['_ingested_at'] = pd.Timestamp.now(tz='UTC')
    return df
