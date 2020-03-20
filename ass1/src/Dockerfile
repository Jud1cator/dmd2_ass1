FROM python:3.8

RUN apt update \
    && apt install -y --no-install-recommends \
        libmariadb-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /src
COPY . .
CMD ["python3", "./main"]
