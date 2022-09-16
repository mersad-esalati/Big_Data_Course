FROM tiangolo/uwsgi-nginx-flask:python3.8

ENV LISTEN_PORT 8585

COPY ./app /app

WORKDIR /app

RUN pip install -r requirements.txt
