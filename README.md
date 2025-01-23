DS3X_BIGQUERY


Descrição
O projeto DS3X_BIGQUERY foi desenvolvido como parte de um teste de admissão para a vaga de Engenheiro de Dados. O objetivo principal deste projeto é realizar a ingestão de dados do site FeComércio para o Google BigQuery, passando por etapas de processamento, criação de tabelas "brutas", "trusted" e "refined".


Estrutura do Projeto
Arquivos Principais
1. ds3x_utils.py: Contém funções auxiliares para o processamento de dados, upload para o BigQuery e a criação das tabelas.

2. ds3x_upload_data_to_bigquery.py: Script principal que executa o pipeline de ingestão de dados, realizando o download, processamento e carregamento no BigQuery.


Etapas do Pipeline de Ingestão
O pipeline de ingestão de dados é dividido nas seguintes etapas:

-> Download de Arquivos:
A função f_download_files realiza o download dos arquivos do site FeComércio para o diretório especificado.

-> Processamento de Arquivos:
A função f_process_files ingesta os arquivos baixados e cria as tabelas brutas no BigQuery.

-> Criação de Tabelas Trusted:
A função f_create_trusted_table é responsável por criar as tabelas "trusted" no BigQuery, que contêm dados confiáveis a partir das tabelas brutas.

-> Criação de Tabelas Refined:
A função f_create_refined_table cria as tabelas refinadas no BigQuery, a partir das tabelas "trusted", com dados mais processados.


Como Executar
Pré-requisitos
-> Python 3.x instalado.
-> Biblioteca Google Cloud BigQuery configurada.
-> Chave de acesso ao Google Cloud com permissões adequadas para criar e manipular tabelas no BigQuery.


Passo a Passo
1. Configuração do Ambiente
   Certifique-se de que o arquivo de chave de acesso ao Google Cloud (SA-william_tomazeto.json) esteja configurado corretamente e tenha as permissões necessárias para o acesso ao BigQuery.

2. Instalação das Dependências
   Instale as dependências do projeto utilizando o pip:
   pip install -r requirements.txt

3. Executando o Script
   Para rodar o script e iniciar o pipeline de ingestão, execute o seguinte comando no terminal, passando os diretórios necessários:
   python ds3x_upload_data_to_bigquery.py "<CAMINHO_DIRETORIO_ARQUIVOS>" "<CAMINHO_CHAVE_ACESSO>"

   Onde:
   <CAMINHO_DIRETORIO_ARQUIVOS> é o diretório onde os arquivos serão lidos e salvos.
   <CAMINHO_CHAVE_ACESSO> é o caminho completo do arquivo de chave de acesso ao Google Cloud (exemplo: "Z:\\DS3X\\SA-william_tomazeto.json").

4. Funções Executadas
   O script executará automaticamente as seguintes funções:

   -> Baixará os arquivos utilizando a função f_download_files.

   -> Processará os arquivos e criará as tabelas brutas com a função f_process_files.

   -> Criará as tabelas "trusted" e "refined" com as funções f_create_trusted_table e f_create_refined_table, respectivamente.

   Exemplo de Execução:
   python ds3x_upload_data_to_bigquery.py "C:\\Users\\william\\Downloads\\ds3x_files\\" "Z:\\DS3X\\SA-william_tomazeto.json"
   
   Esse comando executará todas as etapas do pipeline, desde o download dos arquivos até o processamento e criação das tabelas no BigQuery.

Autor
William Tomazeto

Licença
Este projeto é licenciado sob a DS3X License.
