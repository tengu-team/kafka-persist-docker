FROM python:3.5-slim

WORKDIR /app

ADD . /app

RUN pip install -r requirements.txt

CMD ["python3", "persist.py"]
