FROM python:3.10

WORKDIR /app

COPY event_collector/* .
COPY models.py .
COPY watched_filter.py .

RUN pip install --no-cache-dir -r requirements.txt

ENV NAME EventCollector

CMD ["python", "./main.py"]