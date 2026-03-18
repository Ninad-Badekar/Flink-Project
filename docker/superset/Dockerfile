FROM apache/superset:latest

USER root

RUN python -m ensurepip --upgrade && \
    python -m pip install --no-cache-dir \
    trino \
    sqlalchemy-trino

COPY --chown=superset:superset superset/superset_config.py /app/pythonpath/superset_config.py
COPY --chown=superset:superset superset/init_superset.py /app/init_superset.py
COPY docker/superset/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER superset

EXPOSE 8088

ENTRYPOINT ["/entrypoint.sh"]
