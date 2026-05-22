

FROM python:3.11-slim

WORKDIR /opt/logistics

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements-ml.txt .
RUN pip install --no-cache-dir --timeout=300 -r requirements-ml.txt

COPY . .

ENV PYTHONPATH=/opt/logistics
ENV PYTHONUNBUFFERED=1

RUN useradd --create-home --shell /bin/bash logistics && \
    chown -R logistics:logistics /opt/logistics
USER logistics

EXPOSE 8000

CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
