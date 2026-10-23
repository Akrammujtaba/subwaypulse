# Airflow plus one isolated virtualenv for the jobs it runs, so dbt and Great
# Expectations never fight Airflow's own pinned dependencies.
FROM apache/airflow:2.10.4-python3.11

COPY requirements.txt /tmp/requirements.txt
COPY infra/airflow/jobs-requirements.txt /tmp/jobs-requirements.txt
RUN export PIP_USER=false \
    && python -m venv /opt/airflow/jobs-venv \
    && /opt/airflow/jobs-venv/bin/pip install --no-cache-dir \
       -r /tmp/requirements.txt -r /tmp/jobs-requirements.txt

ENV PYTHONPATH=/opt/subwaypulse
