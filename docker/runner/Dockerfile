FROM python:3.11-slim
COPY docker/runner/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
WORKDIR /workspace
ENV PYTHONPATH=/workspace
CMD ["sleep", "infinity"]
