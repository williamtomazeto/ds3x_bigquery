# Usando uma imagem base
FROM python:3.9

# Set working directory
WORKDIR /app

# Cria o diretório de download e leitura dos arquivos
RUN mkdir -p /app/download
RUN mkdir -p /app/AccessKey

# Copy project files to the container
COPY . /app
COPY SA-william_tomazeto.json /app/AccessKey/

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y wget

# Set default command for the container
#CMD ["python", "ds3x_upload_data_to_bigquery.py", "/app/download/", "/app/AccessKey/SA-william_tomazeto.json"]
CMD ["sh", "-c", "python ds3x_upload_data_to_bigquery.py /app/download/ /app/AccessKey/SA-william_tomazeto.json && tail -f /dev/null"]
