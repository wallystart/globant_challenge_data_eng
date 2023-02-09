FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.0.2
ARG jupyterlab_version=2.1.5

RUN apt-get update -y && \
    apt-get install -y python3-pip libpq-dev gcc jq && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version} sqlalchemy psycopg2-binary psycopg2

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=