# One image for the collector, simulator, schedule loader, and evaluator.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY subwaypulse subwaypulse
ENV PYTHONUNBUFFERED=1
