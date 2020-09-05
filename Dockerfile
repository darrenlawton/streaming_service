FROM python:3.6-alpine

RUN adduser -D pxserve

WORKDIR /home/streaming_service

RUN apt-get update

COPY requirements.txt requirements.txt
RUN python -m venv venv
RUN pip3 install --upgrade pip -r requirements.txt
RUN pip3 install --upgrade pip dependency/IGPrices-1.0-py3-none-any.whl

COPY src src
COPY main.py config.py boot.sh ./
RUN chmod +x boot.sh

RUN chown -R pxserve:pxserve ./
USER pxserve

#ENTRYPOINT ["./boot.sh"]
#CMD [ "python3.6", "stream_launcher.py"]