
FROM apache/superset:latest

USER root

RUN pip install --no-cache-dir "psycopg2-binary>=2.9,<3"

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY --chown=superset:superset --chmod=755 superset-init.sh /usr/local/bin/superset-init.sh
COPY --chown=superset:superset superset_config.py /app/pythonpath/
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset
ENTRYPOINT ["/usr/local/bin/superset-init.sh"]
