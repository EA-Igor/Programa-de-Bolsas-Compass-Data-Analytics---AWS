# Use a imagem oficial do Amazon Linux 2
FROM amazonlinux:latest

# Atualize o sistema e instale os pacotes necessários
RUN yum -y update && \
    yum install -y \
        python3 \
        python3-pip \
        zip \
        && yum clean all

RUN python3 -m pip install --upgrade --ignore-installed pip

RUN python3 -m pip install pandas numpy -t /lambda/python

RUN cd /lambda && zip -r /lambda/layer.zip python