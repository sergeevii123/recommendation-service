FROM python:3.10

WORKDIR /app

COPY recommendations/* .
COPY models.py .
COPY watched_filter.py .

RUN pip install --no-cache-dir -r requirements.txt

ENV NAME RecommendationService

CMD ["python", "./main.py"]