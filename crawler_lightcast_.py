import sqlite3
import requests
import json
import pandas as pd
from tqdm import tqdm
import sys
import os
sys.path.append(os.path.abspath(r"C:\Users\edivaldo.alves\OneDrive\Documentos\VS Code\aca_so"))
from functions.bronze_level_etl_functions_ import who_is_v8, translate_text, limpar_texto
tqdm.pandas()

print('Pacotes importados com sucesso')

client_id = 'sdw5bhg1xut40qht'
secret = 'qzuImRRe'
scope = 'emsi_open'

url = "https://auth.emsicloud.com/connect/token"

payload = f"client_id={client_id}&client_secret={secret}&grant_type=client_credentials&scope={scope}"
headers = {"Content-Type": "application/x-www-form-urlencoded"}

response = requests.request("POST", url, data=payload, headers=headers)
access_token = json.loads(response.text)['access_token']

print(access_token)

# Job Title: https://docs.lightcast.dev/apis/titles#get-list-all-titles

def jobs_process(access_token):

    url = "https://emsiservices.com/titles/versions/latest/titles"

    querystring = {'fields': 'id,name,pluralName,isSupervisor,levelBand,infoUrl'}

    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.request("GET", url, headers=headers, params=querystring)
    response = json.loads(response.text)['data']

    jobs_lightcast = pd.DataFrame(response)

    jobs_lightcast.columns = ['id', 'name', 'plural_name', 'is_supervisor', 'level_band', 'info_url']
    jobs_lightcast['name_regex'] = jobs_lightcast['name'].progress_apply(limpar_texto)
    jobs_lightcast['plural_name_regex'] = jobs_lightcast['plural_name'].progress_apply(limpar_texto)
    jobs_lightcast = jobs_lightcast[['id', 'name', 'name_regex', 'plural_name', 'plural_name_regex', 'is_supervisor', 'level_band', 'info_url']]

    conn = sqlite3.connect(r'database\sources_taxonomy.db')
    jobs_lightcast.to_sql('lightcast_jobs_db', conn, if_exists='replace', index=False)
    conn.close()

# Skills process: https://docs.lightcast.dev/apis/skills#versions-version-skills

def skill_process(access_token):

    url = "https://emsiservices.com/skills/versions/latest/skills"
    querystring = {"typeIds":"ST1,ST2,ST3","fields":"id,name,type,tags,isSoftware,isLanguage,description,descriptionSource,category,subcategory,infoUrl"}

    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.request("GET", url, headers=headers, params=querystring)
    response = json.loads(response.text)['data']

    df = pd.json_normalize(
        response,
        sep='_',
        meta=[
            'id',
            'name',
            'description',
            'descriptionSource',
            'infoUrl',
            'isLanguage',
            'isSoftware',
            ['category', 'id'], 
            ['category', 'name'],
            ['subcategory', 'id'],
            ['subcategory', 'name'],
            ['type', 'id'],
            ['type', 'name']
        ]
    )

    # Renomeando as colunas para um formato mais amigável
    df.columns = [col.replace('.', '_') for col in df.columns]

    df['raw_json'] = df.apply(lambda row: json.dumps(response[df.index.get_loc(row.name)]), axis=1)

    def expand_tags_column(row):
        tags = row['tags']
        if isinstance(tags, list):
            for tag in tags:
                row[tag['key']] = tag['value']
        return row

    # Aplicando a função para cada linha do DataFrame
    df_expanded = df.progress_apply(expand_tags_column, axis=1)

    # Removendo a coluna original de 'tags' se necessário
    df_expanded = df_expanded.drop(columns=['tags'])

    type_dic_description = {'ST1':'Skills that are primarily required within a subset of occupations or equip one to perform a specific task (e.g. \"NumPy\" or \"Hotel Management\"). Also known as technical skills or specialized skills.',
                            'ST2':'Skills that are prevalent across many different occupations and industries, including both personal attributes and learned skills. (e.g. \"Communication\" or \"Microsoft Excel\"). Also known as soft skills, human skills, and competencies.',
                            'ST3':'Certification skills are recognizable qualification standards assigned by industry or education bodies.'}

    columns_choice = ['id', 'name', 'description', 'descriptionSource', 'isLanguage', 'isSoftware', 'category_id', 'category_name'
                    , 'subcategory_id', 'subcategory_name', 'type_id', 'type_name', 'infoUrl', 'wikipediaExtract', 'wikipediaUrl', 'raw_json', ]

    df_expanded['description_type'] = df_expanded['type_id'].map(type_dic_description)

    colunas = ['category_name', 'description', 'name', 'subcategory_name']

    for coluna in colunas:

        if coluna in ['category_name', 'subcategory_name']:
            valores_unicos = df_expanded[coluna].unique()
            df_temp = pd.DataFrame(valores_unicos, columns=[coluna])
        
            df_temp[coluna + '_pt'] = df_temp[coluna].progress_apply(lambda x: translate_text(text=x, source='en', target='pt'))

            df_expanded[coluna + '_pt'] = df_expanded[coluna].map(df_temp.set_index(coluna)[coluna + '_pt'])
        
            df_expanded[coluna + '_regex'] = df_expanded[coluna].progress_apply(limpar_texto)
        else:
            df_expanded[coluna + '_regex'] = df_expanded[coluna].progress_apply(limpar_texto)

    df_expanded.columns = ['category_id', 'category_name', 'description', 'description_source',
        'id', 'info_url', 'is_language', 'is_software', 'name', 'raw_json',
        'subcategory_id', 'subcategory_name', 'type_id', 'type_name',
        'wikipedia_extract', 'wikipedia_url', 'description_type',
        'category_name_pt', 'category_name_regex', 'description_regex',
        'name_regex', 'subcategory_name_pt', 'subcategory_name_regex']

    columns_choice = ['id', 'name', 'name_regex', 'category_id', 'category_name', 'category_name_regex', 'category_name_pt'
                    , 'subcategory_id', 'subcategory_name', 'subcategory_name_pt', 'subcategory_name_regex'
                    , 'description', 'description_regex', 'description_source'
                    , 'is_language', 'is_software', 'type_id', 'type_name'
                    , 'wikipedia_extract', 'wikipedia_url', 'description_type', 'info_url', 'raw_json']

    df_expanded = df_expanded[columns_choice]

    conn = sqlite3.connect(r'database\sources_taxonomy.db')
    df_expanded.to_sql('lightcast_skills_db', conn, if_exists='replace', index=False)
    conn.close()

### chamada

jobs_process(access_token=access_token)

skill_process(access_token=access_token)

