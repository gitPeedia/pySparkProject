FROM openjdk:8-oraclelinux7
LABEL maintainer="Diego Ramos"

# Set env variables
ENV SPARK_HOME /spark-3.1.1-bin-hadoop2.7
ENV PATH $SPARK_HOME:$PATH
ENV PYTHONPATH $SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON python3
#scala -version

RUN yum update -y \
&& yum install -y wget
RUN yum install -y python3-pip \
&& pip3 install py4j

RUN wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm && yum install -y scala-2.11.8.rpm
RUN wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz && tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz

COPY ./ /spark-3.1.1-bin-hadoop2.7/python

WORKDIR /spark-3.1.1-bin-hadoop2.7/python