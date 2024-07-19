import pandas as pd

def clean_data(df):
    # Filter rows based on column: 'Code du dpartement'
    df = df[(df['Code du dpartement'] == "27") | (df['Code du dpartement'] == "76") | (df['Code du dpartement'] == "61") | (df['Code du dpartement'] == "50") | (df['Code du dpartement'] == "14")]
    return df

# Loaded variable 'df' from URI: /Users/newuser/data/elections/raw/candidats_results.parquet
df = pd.read_parquet(r'/Users/newuser/data/elections/raw/candidats_results.parquet')
