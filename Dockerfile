FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt pyproject.toml ./
COPY src/ ./src/
COPY examples/ ./examples/
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app/src
EXPOSE 8000
CMD ["python", "-m", "orchestrator", "list"]
