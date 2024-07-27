""" Simple Python ETL Pipeline example """
import pandas as pd
import requests
from sqlalchemy import create_engine

def extract():
    """ 
    This API extracts data from: http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data 

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the data into the desired structure and filters """
    df = pd.DataFrame(data)
    print(f"The total number of Universities in this API are: {len(data)}")
    df = df[df["name"].str.contains("Minnesota")]
    print(f"The number of Universities in Minnesota: {len(df)}")
    df['domains'] = [','.join(map(str,l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str,l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

def load(df:pd.DataFrame)-> None:
    """ Loads data into an sqlite data base """
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('mn_uni',disk_engine,if_exists='replace')

if __name__ == "__main__":
    data = extract()
    df = transform(data)
    load(df) 