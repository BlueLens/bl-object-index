#FROM bluelens/faiss:ubuntu16-py2
FROM nvidia/cuda:8.0-devel-ubuntu16.04

RUN mkdir -p /usr/src/app
RUN apt-get update -y
RUN apt-get install -y libopenblas-dev python-numpy python-dev swig git python-pip wget

ENV BLASLDFLAGS /usr/lib/libopenblas.so.0


WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH $PYTHONPATH:./faiss

CMD ["python", "main.py"]
