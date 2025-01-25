FROM python:3.12.3

ARG TAP_NAME=''
ARG TAP_FOLDER=TAP_NAME

COPY $TAP_FOLDER /opt/app/$TAP_FOLDER
COPY setup.cfg setup.py tap_entrypoint.sh Dockerfile /opt/app/

WORKDIR /opt/app/

RUN pip install -e .

ENV CONF_LOCATION='' SCHEMA_FILE='' TAP_NAME=$TAP_NAME

RUN  chmod a+x /opt/app/tap_entrypoint.sh

ENTRYPOINT ["/bin/sh", "/opt/app/tap_entrypoint.sh"]