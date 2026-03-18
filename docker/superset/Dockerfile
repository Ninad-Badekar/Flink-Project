FROM apache/superset:latest

USER root

RUN python -m ensurepip --upgrade && \
    python -m pip install --no-cache-dir \
    trino \
    sqlalchemy-trino

USER superset
