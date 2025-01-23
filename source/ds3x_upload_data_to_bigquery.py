#encoding: iso-8859-1 -- PROBLEMA RESOLVIDO COM A ISO

#ds3x_upload_data_to_bigquery

from ds3x_utils import f_process_files, f_create_dataframe, f_bigquery_connection, f_upload_bigquery,f_download_files, f_create_trusted_table, f_create_refined_table
import argparse
import os

parser = argparse.ArgumentParser(description="Permitir a passagem de dois parâmetros (diretório de escrita e leitura dos arquivos e diretório da chave de acesso) para tornar o código mais flexível")

parser.add_argument('file_path', type=str, help='Diretório para escrita e leitura de arquivos')
parser.add_argument('access_key_path', type=str, help='Diretório da chave de acesso')

args = parser.parse_args()

# exemplo de chamada no terminal
#python Z:\\TESTE_VENA\\python\\ds3x_upload_data_to_bigquery.py "C:\\Users\\william\\Downloads\\ds3x_files\\" "Z:\\DS3X\\SA-william_tomazeto.json"

f_download_files(args.file_path)
f_process_files(args.file_path, args.access_key_path)
f_create_trusted_table(f_bigquery_connection(args.access_key_path))
f_create_refined_table(f_bigquery_connection(args.access_key_path))
