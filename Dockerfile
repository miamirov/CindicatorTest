FROM python:3.8 as builder
COPY requirements.txt .

RUN python3 -m pip install -r requirements.txt

COPY ./src .
CMD ["python3", "./main.py" ]