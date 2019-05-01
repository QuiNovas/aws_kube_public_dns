FROM alpine:3.9
LABEL maintainer="Mathew Moon <me@mathewmoon.net>"


RUN mkdir /app

RUN apk add --no-cache python py-pip bash && \
    pip install boto3 dnspython kubernetes && \
    pip install --upgrade urllib3 && \
    pip install --upgrade requests && \
    apk del --no-cache py-pip

COPY AWS_EXTERNAL_DNS_UPDATER.py main.py /app/

RUN chmod +x /app/*.py

WORKDIR /app

ENTRYPOINT ["/app/main.py"]
