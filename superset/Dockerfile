FROM apache/superset:latest

USER root

RUN python -m ensurepip --upgrade \
 && python -m pip install --no-cache-dir \
    snowflake-sqlalchemy \
    snowflake-connector-python

USER superset