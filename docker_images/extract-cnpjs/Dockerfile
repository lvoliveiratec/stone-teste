FROM python:3.12.3

# Defina o nome do tap
ARG TAP_NAME=""
ARG TAP_FOLDER=$TAP_NAME

# Copie o diretório do tap para dentro da imagem
COPY $TAP_FOLDER /opt/app/$TAP_FOLDER

# Copie o setup.py, o entrypoint e o Dockerfile
COPY setup.py tap_entrypoint.sh Dockerfile /opt/app/

# Defina o diretório de trabalho
WORKDIR /opt/app/

# Instale as dependências definidas no setup.py
RUN pip install .

# Defina as variáveis de ambiente
ENV CONF_LOCATION='' SCHEMA_FILE='' TAP_NAME=$TAP_NAME
ENV PYTHONPATH="/opt/app/extract_cnpjs:$PYTHONPATH"

# Garanta que o entrypoint tenha permissão de execução
RUN chmod a+x /opt/app/tap_entrypoint.sh

# Defina o entrypoint
ENTRYPOINT ["/bin/sh", "/opt/app/tap_entrypoint.sh"]
