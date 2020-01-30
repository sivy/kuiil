FROM python:3.7-alpine as base

RUN apk update

FROM base as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir pipenv
RUN pip install --install-option="--prefix=/install" -r /requirements.txt

FROM base

COPY --from=builder /install /usr
COPY src /app

WORKDIR /app

CMD ["python", "trenchrun.py"]
