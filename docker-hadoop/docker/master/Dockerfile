FROM irm/hadoop-cluster-base
MAINTAINER irm

WORKDIR /root

# install Spark
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-2.4.2/spark-2.4.2-bin-hadoop2.7.tgz && \
tar -xvzf spark-2.4.2-bin-hadoop2.7.tgz && mv spark-2.4.2-bin-hadoop2.7 /usr/local/spark && rm spark-2.4.2-bin-hadoop2.7.tgz

ENV PATH=$PATH:/usr/local/spark/bin
ENV SPARK_HOME=/usr/local/spark
ENV LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH

ADD config/spark-defaults.conf /usr/local/spark/conf
RUN chown root:root /usr/local/spark/conf/spark-defaults.conf

# Añadir mi jar
# ADD bin/stackanswer_2.12-1.0.jar /usr/local/spark/jars

ADD config/bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV BOOTSTRAP /etc/bootstrap.sh

VOLUME /data

EXPOSE 18080

CMD ["/etc/bootstrap.sh", "-d"]
