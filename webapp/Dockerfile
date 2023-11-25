FROM python:3.10

WORKDIR /app

COPY app.py .
COPY requirements.txt .
COPY templates ./templates

RUN pip install --no-cache-dir -r requirements.txt

ENV NAME Webapp

CMD ["python", "./app.py"]