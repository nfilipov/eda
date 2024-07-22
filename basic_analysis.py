import pandas as pd

#europeennes
df_2019 = pd.read_parquet('~/data/elections/eda/minieda/Vernon/2019_euro_t1.parquet')
df_2024_e =pd.read_parquet('~/data/elections/eda/minieda/Vernon/2024_euro_t1.parquet')

# Exemple de dict d'analyse sur une commune
basic_dict_part = {
    'Inscrits' : 'first',
    'Abstentions': 'first', 
    'Nuls' : 'first' , 
    'Abstentions' : 'first',
    'Exprimés': 'first', 
    'Voix' : 'sum'
    }

def make_grouped_datasets(df1):
    grouped_1 = df1.groupby('Code du b.vote_df1').agg(basic_dict_part).reset_index()
    #grouped_2 = df2.groupby('Code du b.vote_df1').agg(dict_part).reset_index()
    return grouped_1

def summarize_participation_any(df):
    participation_ = (df['Inscrits'] - df['Abstentions'])/ df['Inscrits']
    Inscrits_ville = df['Inscrits'].sum()
    Abstentions_ville = df['Abstentions'].sum()
    Participation_ville = (Inscrits_ville-Abstentions_ville)/Inscrits_ville
#    s = " Élections européennes 2019 Vernon:"
    s = str(Inscrits_ville)+" Abstentions :"+str(Abstentions_ville),"Participation :"+ format(100*Participation_ville,'.1f')+"%"
    return s


grouped_2019 = make_grouped_datasets(df_2019.copy())
grouped_2024_e = make_grouped_datasets(df_2024_e.copy())

print(" Élections européennes 2019 Vernon:\n",summarize_participation_any(grouped_2019.copy()))
print(" Élections européennes 2024 Vernon:\n",summarize_participation_any(grouped_2024_e.copy()))