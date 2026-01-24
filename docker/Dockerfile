FROM python:3.12-slim

WORKDIR /app

COPY fc_data_streaming/ /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "producer.py"]


