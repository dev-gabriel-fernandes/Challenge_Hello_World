# =============================================================================
# LIBRARYS
# =============================================================================

import pandas as pd
from unicodedata import normalize
import gender_guesser.detector as gender
import plotly.express as px
from os.path import exists
from os import makedirs,environ
from datetime import datetime
import logging
from sqlalchemy import create_engine



# =============================================================================
# FUNCTIONS
# =============================================================================


def _get_gender(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    gd = gender.Detector()
    dataframe['GENDER'] = dataframe[column].str.split('_').str[0].str.title().apply(gd.get_gender).str.upper()

    return dataframe
    
def structured_logging_info(suppress_spark: bool = True):
    """ Define a clear structure to the logging, and suppress the spark py4j if needed
    Args:
    suppress_spark: Boolean value informing if the logging of py4j will be in the warning level
    Returns:
    logger: The logger for the module
    """
    # Supress the INFO logging of spark python for java
    loggerSpark = logging.getLogger('py4j')
    loggerSpark.setLevel('WARNING')
    
    logger = logging.getLogger()
    
    # Formate the logger
    formatter = logging.Formatter('Line %(lineno)d : [ %(asctime)s ] %(filename)s/%(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # Configure stream handler for the cells
    chandler = logging.StreamHandler()
    chandler.setLevel(logging.INFO)
    chandler.setFormatter(formatter)
    
    logger.handlers = []
    logger.addHandler(chandler)
    logger.setLevel(logging.INFO)
    
    return logger

logger = structured_logging_info()

def _remove_special_characters(text: str) -> str:
    if type(text) == str:
        return normalize('NFKD', text.upper()).encode('ASCII', 'ignore').decode('ASCII')
    else:
        return text

def _cluster_hour(x):

    if x in range(0,6): return 'MADRUGADA'
    if x in range(6,12): return 'MANHA'
    if x in range(12,18): return 'TARDE'
    if x in range(18,24): return 'NOITE'


# =============================================================================
# IMPORTANDO DATAFRAMES
# =============================================================================

def _import_dataframes() -> pd.DataFrame:
    card_brand = pd.read_csv('./card_brand.csv')
    card_brand.columns = card_brand.columns.str.upper()
    
    transactions = pd.read_csv('./transactions.csv', sep = ';')
    transactions.columns = transactions.columns.str.upper()

    # =============================================================================
    # UNIFICANDO ARQUIVOS E RENOMEANDO
    # =============================================================================
    
    df = transactions.merge(card_brand, how = 'left', left_on='CARD_BRAND', right_on='BRAND_CODE').drop(['CARD_BRAND','ID_y'], axis =1)
    
    df.columns = ['TRANSACTION_ID', 'CREATED_AT', 'MERCHANT_ID', 'VALUE', 'INSTALLMENTS',
           'CARD_NAME', 'STATUS', 'CARD_ID', 'ISO_ID', 'DOCUMENTS',
           'BRAND_NAME', 'BRAND_CODE']

    return df

def _create_columns_features(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    # =============================================================================
    # EXTRAINDO INFORMAÇÕES DE DATA E HORA
    # =============================================================================
    dataframe['CREATION_HOUR'] = pd.to_datetime(dataframe[column]).dt.hour
    dataframe['CREATION_DAY'] = pd.to_datetime(dataframe[column]).dt.day
    dataframe['CREATION_MONTH'] = pd.to_datetime(dataframe[column]).dt.month
    dataframe['CREATION_YEAR'] = pd.to_datetime(dataframe[column]).dt.year
    dataframe['DAY_PERIOD'] = dataframe.CREATION_HOUR.apply(_cluster_hour)

    return dataframe

def _data_cleaning(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
# =============================================================================
# DATA CLEANING
# =============================================================================
    for i in columns:
        dataframe[i] = dataframe[i].str.replace(' ','_').apply(_remove_special_characters).fillna('UNINFORMED')
    
    return dataframe


def _print_frequence_table(dataframe: pd.DataFrame, columns: list):
    
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    for i in columns:
        
        print(
            '''
# =============================================================================
# PRINTING STATISTICAL DESCRIPTION: VALUE X''',i,'''
# =============================================================================
            '''
            )
        
        description = dataframe[['VALUE',i]].groupby([i]).agg(['sum', 'count','mean', 'min','max']).sort_values(('VALUE','sum'), ascending = False)
        description.to_csv('./frequence_table/Frequence_table_'+i+'.csv', sep = ';', decimal = ',')
        print(description.iloc[:10,:])

    return

def _create_report_bi(dataframe: pd.DataFrame, columns: list):
    for i in columns:
        fig1 = px.box(dataframe, y="VALUE", x=i, title = 'Boxplot VALUE X '+i)
        fig2 = px.histogram(dataframe, y="VALUE", x=i, title = 'Histogram VALUE X'+i)

        with open("./graphs/Report_"+i+".html",'w') as f:
            f.write(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write(fig2.to_html(full_html=False, include_plotlyjs='cdn'))
    
    return

def _creat_folders():
    path = ['./graphs', './frequence_table', './results']
    
    for i in path:
        if not exists(i):
            makedirs(i)
    return

def df_to_sql(dataframe: pd.DataFrame, table_name: str,user: str, host: str, database: str):
        
    engine_destino = create_engine('mysql://'+ user +':'+ environ.get('PASS_AURORA') +'@'+ host +'/'+ database)
    
    dataframe.to_sql(name=table_name, con=engine_destino,
        if_exists='append', index=False, chunksize=10000)

    return

def run() -> pd.DataFrame:
    
    date = datetime.now().now()
    date = str(date.year) + str(date.month).zfill(2) + str(date.day).zfill(2) + str(date.hour).zfill(2) + str(date.minute).zfill(2)
    
    logger.info('Criando pastas')
    _creat_folders()
    logger.info('Importando dataframes')
    df = _import_dataframes()
    df = _create_columns_features(df,'CREATED_AT')
    logger.info('Padronizando dados')
    df = _data_cleaning(df, columns = ['CARD_NAME', 'STATUS', 'BRAND_NAME'])
    df = _get_gender(df, 'CARD_NAME')
    logger.info('Printando descrições estatísticas')
    _print_frequence_table(df,['STATUS', 'CARD_ID', 'ISO_ID', 'BRAND_NAME', 'DAY_PERIOD', 'GENDER'] )
    _create_report_bi(df,['STATUS', 'CARD_ID', 'ISO_ID', 'BRAND_NAME', 'DAY_PERIOD', 'GENDER'] )
    df.to_csv('./results/Result_' + date + '.csv', sep =';', decimal=',')
    logger.info('Arquivos exportados')
    
    return df

if __name__ == '__main__':
    run()
