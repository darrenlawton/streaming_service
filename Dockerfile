FROM python:3.6

RUN adduser pxserve

WORKDIR /home/streaming_service
RUN mkdir logs
RUN mkdir holding_folder


COPY requirements.txt requirements.txt
COPY dependency/IGPrices-1.0-py3-none-any.whl dependency/IGPrices-1.0-py3-none-any.whl

RUN python -m venv venv
RUN venv/bin/pip install --upgrade pip
RUN venv/bin/pip install -r requirements.txt
RUN venv/bin/pip install dependency/IGPrices-1.0-py3-none-any.whl

COPY src src
COPY main.py config.py ./

RUN chown -R pxserve:pxserve ./
USER pxserve

ENTRYPOINT ["venv/bin/python","main.py"]

# python3 main.py -e CS.D.BITCOIN.CFD.IP CS.D.ETHUSD.CFD.IP -d 6/9/2020 -f 13 36 -t 60 -s