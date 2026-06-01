FROM ghcr.io/mlflow/mlflow:v3.10.1

RUN pip install --no-cache-dir psycopg2-binary

# show installed versions in logs for debugging purposes
RUN python -c "import mlflow, psycopg2; print('mlflow', mlflow.__version__, 'psycopg2', psycopg2.__version__)"