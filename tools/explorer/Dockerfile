FROM python:3.11.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /opt/explorer
COPY tools/explorer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --only-binary=pyarrow,fastavro -r requirements.txt \
    && useradd --uid 10001 --create-home explorer
COPY tools/explorer/app ./app
USER explorer
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
