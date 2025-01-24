#encoding: iso-8859-1

#ds3x_utils
 
from  google.auth.exceptions import GoogleAuthError
import google.auth.exceptions 
import pandas_gbq as pgbq
import pandas as pd
import os
from google.oauth2 import service_account
from typing import Optional, Union, List, Dict
from google.cloud import bigquery
import wget
import os
import requests 
from bs4 import BeautifulSoup


def f_create_dataframe(pFilePath: str, pSheetName: Optional[Union[str, int]]=0, pSkipRows: Optional[int]=0, pHeader: Optional[int]=0, pColumns: Optional[List[str]]=None)-> pd.DataFrame:
    """
    A função f_create_dataframe gera um dataframe que pode ser usando, por exemplo, na criação de tabelas no Google Bigquery

    Parâmetros:
    pFilePath (str): Diretório onde um arquivo Excel será lido
    pSheetName (Optional[Union[str, int]]=0): Ñome ou posição da planilha a ser utilizada no arquivo Excel
    pSkipRows (Optional[int]=0): Número de linhas de salto/desprezadas no início do arquivo
    pHeader (Optional[int]=0): Indica se a primeira linha será ou não utilizada como cabeçalho
    pColumns (Optional[List[str]]=None): Lista de colunas opicional para alteração dos nomes originais

    Retorna:
    pd.DataFrame: A função retorna um dataframe pandas
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """

    try:
        # Carregando o arquivo num Data Frame
        df = pd.read_excel(pFilePath, sheet_name=pSheetName, skiprows=pSkipRows, header=pHeader)
        
        # Atribuindo os novos nomes de colunas
        df.columns = pColumns

        # Adicionando a coluna LOAD_TIMESTAMP para refenciar a data da carga
        df['LOAD_TIMESTAMP'] = pd.to_datetime('now')

        return df
    except Exception as e:
        return pd.DataFrame()


def f_bigquery_connection(pPathAccessKey: str)->Dict[str, object]:
    """
    A função f_bigquery_connection cria a conexão com o recurso Bigquery

    Parâmetros:
    pPathAccessKey (str): Diretório acompanhdo do nome do arquivo de conta de serviço para acesso à api do bigquery
    
    Retorna:
    Dict: A função retorna um dicionário com as informações de credentils, client, project_id e dataset_id
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """

    try:
        # Configurando Credenciais
        credentials = service_account.Credentials.from_service_account_file(
        pPathAccessKey, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Configurando o Client
        client = bigquery.Client(
             credentials=credentials,
             project=credentials.project_id,
        )

        # Listando os datasets disponíveis 
        datasets = list(client.list_datasets())
        for dataset in datasets:
            vDataSetID = dataset.dataset_id

        return {
        'credentials': credentials,
        'client': client,        
        'project_id': client.project,
        'dataset_id': vDataSetID,       
        }            
        
        print('Credenciais criadas com sucesso!')
    except FileNotFoundError:
        print(f"Erro: O arquivo '{pPathAccessKey}' não foi encontrado.")
    except ValueError as ve:
        print(f"Erro: Arquivo de credenciais inválido. Detalhes: {ve}")
    except google.auth.exceptions.DefaultCredentialsError as auth_err:
        print(f"Erro de autenticação: {auth_err}")
    except Exception as e:
        print(f"Erro inesperado: [e]")


def f_upload_bigquery(pDf: pd.DataFrame,  pConnection: Dict, pTable: str):    
    """
    A função f_upload_bigquery faz o upload dos arquivos excel lidos para o bigquery

    Parâmetros:
    pDf (pd.DataFrame): Dataframe com os dados do arquivo 
    pConnection: (Dict): Dicionário com informações do ambiente bigquery
    pTable (str): Tabela a ser criada
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """    
    try:
        # Configurando Credenciais e Projeto
        pgbq.context.credentials = pConnection['credentials']
        pgbq.context.project = pConnection['project_id']
    
        # Upload de arquivo para Bigquery
        pgbq.to_gbq(pDf, f"{pConnection['dataset_id']}.{pTable}", project_id=pConnection['project_id'], if_exists='replace')
        print(f"Tabela {pTable} criada ou substituída com sucesso!\n")
    
        # Lista as tabelas do Dataset
        tables = list(pConnection['client'].list_tables(pConnection['dataset_id']))
        print(f"O Dataset {pConnection['dataset_id']} abriga as seguintes tabelas:")
        if tables:
            for table in tables:
                print(f"- {table.table_id}")
        else:
            print(f"O dataset {pConnection['dataset_id']} está vazio")
   
    except GoogleAuthError as auth_err:
        print(f"Erro de autenticação: {auth_err}")
    except bigquery.exceptions.NotFound as nf_err:
        print(f"Erro: Dataset ou tabela não encontrado. Detalhes: {nf_err}")
    except ValueError as ve:
        print(f"Erro de valor: Verifique os parâmetros fornecidos. Detalhes: {ve}")
    except Exception as e:
        print(f"Erro inesperado: {e}")    

    print(f"\n")    


def f_process_files(pPath: str, pPathAccessKey: str):
    """
    A função f_upload_bigquery faz o upload dos arquivos excel lidos para o bigquery

    Parâmetros:
    pPath (str): Diretório de leitura dos arquivos excel a serem ingeridos
    pPathAccessKey (str): Diretório onde se encontra o arquivo contendo a chave de acesso para o bigquery
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """    

    try:
        #Lista os arquivos do diretório
        files = [f for f in os.listdir(pPath) if os.path.isfile(os.path.join(pPath, f)) and f.endswith('.xlsx')]
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado no diretório: {pPath}")

        for file in files:
            try:
                print(f"Processando o arquivo {file}")
                
                # Coletando as 3 primeiras letras do nome do arquivo para formar o nome da tabela RAW a ser criada
                table_raw = file[:3]+'_raw'
                
                # Nomes de colunas que substituirão os nomes originais 
                DictColumns = {'ICCColumns': ['MES', 'ICC', 'ICCATE10SM', 'ICC>10SM', 'ICCHOMENS', 'ICCMULHERES', 'ICCATE35ANOS', 'ICC>35ANOS', 'ICEA', 'ICEAATE10SM', 'ICEA>10SM', 'ICEAHOMENS', 'ICEAMULHERES', 'ICEAATE35ANOS', 'ICEA>35ANOS', 'IEC', 'IECATE10SM', 'IEC>10SM', 'IECHOMENS', 'IECMULHERES', 'IECATE35ANOS', 'IEC>35ANOS'],
                               'ICFColumns': ['MES', 'ICF', 'ICFATE10SM', 'ICF>10SM', 'EMPATUAL', 'EMPATUALATE10SM', 'EMPATUAL>10SM', 'PERSPPROF', 'PERSPPROFATE10SM', 'PERSPPROF>10SM', 'RENDAATUAL', 'RENDAATUALATE10SM', 'RENDAATUAL>10SM', 'ACESCRED', 'ACESCREDATE10SM', 'ACESCRED>10SM', 'NIVCONSATUAL', 'NIVCONSATUALATE10SM', 'NIVCONSATUAL>10SM', 'PERSPCONS', 'PERSPCONSATE10SM', 'PERSPCONS>10SM', 'MOMDURAV', 'MOMDURAVATE10SM', 'MOMDURAV>10SM']
                              }
                
                # Colocando em maiúsculo o prefixo que será utilizado como chave no dicionário DictColumns para  retornar o conjutno de colunas correto
                vPrefix = file[:3].upper()

                # Utiliza as f_create_dataframe e f_bigquery_connection com parâmetros para f_upload_bigquery d euload de arquivos para o Bigquery
                f_upload_bigquery(f_create_dataframe(pPath + file, 1, 1, 0, DictColumns[vPrefix + 'Columns']),  f_bigquery_connection(pPathAccessKey), table_raw)

            except KeyError as ke:
                print(f"Erro: {ke}")
            except Exception as e:
                print(f"Erro ao processar o arquivo '{file}': {e}")

    except FileNotFoundError as fnf_err:
        print(f"Erro: {fnf_err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


def f_download_files(pPath: str):
    """
    A função f_download_files faz o download dos arquivos do site FeComércio

    Parâmetros:
    pPath (str): Diretório de escrita onde os arquivos excel serão baixados
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """    

    # Lista de páginas que contém links para downloads
    donwload_urls = ['https://www.fecomercio.com.br/pesquisas/indice/icc', 'https://www.fecomercio.com.br/pesquisas/indice/icf']

    # Local para armazenar os arquivos.
    #vfilepath = 'C:\\Users\\william\\Downloads\\ds3x_files\\'

    # Realizando o download dos arquivos 
    for url in donwload_urls:
        try:
            # Request da página web
            html = requests.get(url)
        
            # Captura status da página
            html.raise_for_status()

        except requests.exceptions.HTTPError as http_err:
             print(f"Erro HTTP: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
             print(f"Erro de conexão: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
             print(f"Erro de timeout: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
             print(f"Erro na requisição: {req_err}")
        except Exception as e:
            print(f"Erro inesperado: {e}")
        else:
        
            # parse do html para localização do link de download
            soup = BeautifulSoup(html.content, 'html.parser')
        
            # Coleta o link de download
            download_link = soup.find("a",{"class":"download"}).get("href")
        
            # 
            filename = pPath + os.path.basename(download_link)
            if os.path.exists(filename):
                os.remove(filename)
            wget.download(download_link, filename)

    print('\nDownload de arquivos realizado com sucesso!\n')

def f_create_trusted_table(pConnection: Dict):
    """
    A função f_create_trusted_table cria as tabelas trusted

    Parâmetros:
    pConnection (Dict): Dicionário contendo informações do ambinte bigquery
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """    

    tables = list(pConnection['client'].list_tables(pConnection['dataset_id']))
    if tables:
        for table in tables:
            SourceTable = f"{table.table_id}"
            if 'raw' in SourceTable:
                
                # Monta o nome totalmente qualificado da tabela
                SourceTableID = f"{pConnection['project_id']}.{pConnection['dataset_id']}.{SourceTable}"
                TargetTable = SourceTable.replace("raw", "trusted")
                TargetTableID = f"{pConnection['project_id']}.{pConnection['dataset_id']}.{TargetTable}"

                # Consulta a tabela de origem
                sql_query = f"""
                    SELECT DISTINCT *
                    FROM `{SourceTableID}`
                    """
                
                try:
                    # Configuração para criação ou substituição da tabela
                    job_config = bigquery.QueryJobConfig(destination=TargetTableID)
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE 

                    # Executa a consulta para criar ou substituir a tabela
                    query_job = pConnection['client'].query(sql_query, job_config=job_config)
                    query_job.result()  # Aguarda a execução da consulta   
                    print(f"Tabela destino {TargetTableID} criada ou substituída com sucesso!\n")
                except Exception as e:
                    print(f"Erro no processamento da tabela destino {TargetTableID}\n")                    


def f_create_refined_table(pConnection: Dict):
    """
    A função f_create_refined_table cria a tabela refined

    Parâmetros:
    pConnection (Dict): Dicionário contendo informações do ambinte bigquery
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """        
    # Consulta tabelas trusted para criação da tabela refined
    sql = f"""
    WITH 
    icc_with_date AS (
        SELECT
            *,
            FORMAT_TIMESTAMP('%Y-%m', `MES`) AS YYYY_MM  -- Formata a data para YYYY-MM
        FROM `{pConnection['dataset_id']}.{'icc_trusted'}`
    ),
    icf_with_date AS (
        SELECT
            *,
            FORMAT_TIMESTAMP('%Y-%m', `MES`) AS YYYY_MM  -- Formata a data para YYYY-MM
        FROM `{pConnection['dataset_id']}.{'icf_trusted'}`
    ),
    joined_data AS (
        SELECT 
            icc.YYYY_MM AS YYYY_MM,
            icc.ICC,
            icc.ICEA,
            icc.IEC,
            icf.ICF
        FROM icc_with_date icc
        INNER  JOIN icf_with_date icf
            ON icc.YYYY_MM = icf.YYYY_MM  -- O JOIN pode ser ajustado conforme a chave comum entre as tabelas
    ),
    variation AS (
        SELECT
            YYYY_MM,
            ICC,
            ROUND(100 * (ICC - LAG(ICC) OVER (ORDER BY YYYY_MM)) / LAG(ICC) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICC,
            ICEA,
            ROUND(100 * (ICEA - LAG(ICEA) OVER (ORDER BY YYYY_MM)) / LAG(ICEA) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICEA,           
            IEC,
            ROUND(100 * (IEC - LAG(IEC) OVER (ORDER BY YYYY_MM)) / LAG(IEC) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_IEC,           
            ICF,            
            ROUND(100 * (ICF - LAG(ICF) OVER (ORDER BY YYYY_MM)) / LAG(ICF) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICF,
            DATETIME(CURRENT_TIMESTAMP(), "America/Sao_Paulo") LOAD_TIMESTAMP
        FROM joined_data
    )
    SELECT * FROM variation
    """       

    TargetTableID = f"{pConnection['project_id']}.{pConnection['dataset_id']}.{'icf_icc_refined'}"

    try:
        # Configuração para criação ou substituição da tabela
        job_config = bigquery.QueryJobConfig(destination=TargetTableID)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE 

        # Executa a consulta para criar ou substituir a tabela
        query_job = pConnection['client'].query(sql, job_config=job_config)
        query_job.result()  # Aguarda a execução da consulta   
        print(f"Tabela destino {TargetTableID} criada ou substituída com sucesso!\n")
    except Exception as e:
        print(f"Erro no processamento da tabela destino {TargetTableID}\n")                    


def query_bigquery(pConnection: Dict):
    """
    A função query_bigquery consulta tabelas no bigquery, de maneira
    que a tabela deve ser indicada na consulta escrita na variável sql.
    Usada apenas para conferência e não faz parte do rpocesso de automatização.

    Parâmetros:
    pConnection (Dict): Dicionário contendo informações do ambinte bigquery
    
    Retorna:
    None: Sem retorno
    """

    content = f"""
    # DS3X_BIGQUERY
    
    ## Descrição
    O projeto DS3X_BIGQUERY foi aplicado com teste de admissão para a vaga de Engenheiro de Dados
    
    ## Autor
    William Tomazeto
    """        
    client = pConnection['client']
    
    print(f"{pConnection['project_id']}.{pConnection['dataset_id']}.{'icc_trusted'}")
    
    sql = f"""    
       select * from `{pConnection['dataset_id']}.{'icf_icc_refined'}`
       """
    
    # sql = f"""
    # WITH 
    # icc_with_date AS (
    #     SELECT
    #         *,
    #         FORMAT_TIMESTAMP('%Y-%m', `MES`) AS YYYY_MM  -- Formata a data para YYYY-MM
    #     FROM `{pConnection['dataset_id']}.{'icc_trusted'}`
    # ),
    # icf_with_date AS (
    #     SELECT
    #         *,
    #         FORMAT_TIMESTAMP('%Y-%m', `MES`) AS YYYY_MM  -- Formata a data para YYYY-MM
    #     FROM `{pConnection['dataset_id']}.{'icf_trusted'}`
    # ),
    # joined_data AS (
    #     SELECT 
    #         icc.YYYY_MM AS YYYY_MM,
    #         icc.ICC,
    #         icc.ICEA,
    #         icc.IEC,
    #         icf.ICF
    #     FROM icc_with_date icc
    #     INNER  JOIN icf_with_date icf
    #         ON icc.YYYY_MM = icf.YYYY_MM  -- O JOIN pode ser ajustado conforme a chave comum entre as tabelas
    # ),
    # variation AS (
    #     SELECT
    #         YYYY_MM,
    #         ICC,
    #         ROUND(100 * (ICC - LAG(ICC) OVER (ORDER BY YYYY_MM)) / LAG(ICC) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICC,
    #         ICEA,
    #         ROUND(100 * (ICEA - LAG(ICEA) OVER (ORDER BY YYYY_MM)) / LAG(ICEA) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICEA,           
    #         IEC,
    #         ROUND(100 * (IEC - LAG(IEC) OVER (ORDER BY YYYY_MM)) / LAG(IEC) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_IEC,           
    #         ICF,            
    #         ROUND(100 * (ICF - LAG(ICF) OVER (ORDER BY YYYY_MM)) / LAG(ICF) OVER (ORDER BY YYYY_MM), 2) AS PERC_VAR_ICF,
    #         DATETIME(CURRENT_TIMESTAMP(), "America/Sao_Paulo") LOAD_TIMESTAMP
    #     FROM joined_data
    # )
    #  SELECT * FROM variation
    # """

    # Executando a consulta
    query_job = client.query(sql)  # Aqui passamos a consulta SQL como parâmetro
    results = query_job.result()  # Espera o resultado da consulta
    
    # Convertendo os resultados para um DataFrame
    df = results.to_dataframe()
    
    # Exibindo os resultados na tela
    print(df)


# Bloco de código de teste das funções
if __name__ == "__main__":
    vPathAccessKey = "Z:\\DS3X\\SA-william_tomazeto.json"
    vfilepath = 'C:\\Users\\william\\Downloads\\ds3x_files\\'
    #f_download_files(vfilepath)
    #f_process_files(vfilepath, vPathAccessKey)

    # Chamando a função para consultar e exibir os resultados
    query_bigquery(f_bigquery_connection(vPathAccessKey))







