# Usar uma imagem base do Python
FROM python:3.9-slim

# Definir o diretório de trabalho no container
WORKDIR /app

# Copiar o requirements.txt para o container
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código fonte para o container
COPY . .

# Comando para rodar o script
CMD ["python", "ds3x_upload_data_to_bigquery.py"]