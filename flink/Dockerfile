FROM flink:1.20.1

USER root

# Install Python and pip
RUN apt-get update \
    && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Install PyFlink
RUN python3 -m pip install apache-flink==1.20.1

# Copy Flink application
COPY crypto_stream.py /opt/flink/usrlib/crypto_stream.py

USER flink