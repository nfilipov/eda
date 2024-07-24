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

def make_grouped_datasets(df1,key:None,dict:None ):
    # this function groups and aggregates results based on a given key and an agg dictionary.
    # use case : participation to euro elections
    # BV, Liste, Inscrits, Voix
    # 1 , List1, 30, 14
    # 1 , List2, 30, 12
    # 2 , List1, 21, 4
    # 2 , List2, 21, 12
    # this function can be applied as make_grouped_datasets(df, BV, dict={'Inscrits':'first','Voix':'sum'})
    # and will return the following 'grouped' dataset:
    # grouped  = DataFrame (
    #   'BV' : 1, 2
    #   'Inscrits' : 30, 21
    #   'Voix' : 26, 16
    # )
    if key == None:
        key='Code du b.vote_df1'
    if dict == None:
        dict = basic_dict_part
    grouped_1 = df1.groupby(key).agg(dict).reset_index()
    return grouped_1

def summarize_participation_any(df):
    Inscrits_ville = df['Inscrits'].sum()
    Abstentions_ville = df['Abstentions'].sum()
    Participation_ville = (Inscrits_ville-Abstentions_ville)/Inscrits_ville
#    s = " Élections européennes 2019 Vernon:"
    s = str(Inscrits_ville)+" Abstentions :"+str(Abstentions_ville),"Participation :"+ format(100*Participation_ville,'.1f')+"%"
    return s


#grouped_2019 = make_grouped_datasets(df_2019.copy(), 'Code du b.vote_df1',basic_dict_part)
#grouped_2024_e = make_grouped_datasets(df_2024_e.copy(), 'Code du b.vote_df1',basic_dict_part)

#print(" Élections européennes 2019 Vernon:\n",summarize_participation_any(grouped_2019.copy()))
#print(" Élections européennes 2024 Vernon:\n",summarize_participation_any(grouped_2024_e.copy()))

def merge_df(df1,df2,suffix1, suffix2,title=None):
    if title == None:
        title='_tmp'

    df = pd.merge(
        df1, df2,
        on=["Code du b.vote_df1"],
        suffixes=[suffix1, suffix2]
    )
    return df

#europeennes_part = merge_df(grouped_2019.copy(),grouped_2024_e.copy(),"_2019","_2024",'europeennes_part_vernon')


