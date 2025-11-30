FROM apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu

USER root
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir kafka-python

USER spark