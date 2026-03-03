FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY infra/azure/requirements-job.txt /tmp/requirements-job.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements-job.txt

COPY . /app

CMD ["python", "statt/run_daily_committee_pipeline.py"]
