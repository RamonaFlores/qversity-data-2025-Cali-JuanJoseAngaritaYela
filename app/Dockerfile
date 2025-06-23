FROM python:3.11-slim

WORKDIR /app

# 🔐 Usa el requirements de FastAPI exclusivamente
COPY ./app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/app
COPY ./app/ ./
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]


