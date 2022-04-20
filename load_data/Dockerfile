FROM python:3.9-alpine

WORKDIR /code
COPY ./load_data/src .

COPY ./load_data/requirements.txt ./requirements.txt
RUN pip install --upgrade --no-cache-dir pip && pip install --no-cache-dir -r requirements.txt

COPY ./load_data/wait-for.sh .