FROM bde2020/spark-java-template:2.4.0-hadoop2.7

ENV SPARK_APPLICATION_JAR_NAME TopicConsumer-0.0.1-SNAPSHOT-jar-with-dependencies
ENV SPARK_APPLICATION_MAIN_CLASS sparkstreaming.TopicConsumer
ENV ENABLE_INIT_DAEMON false

ONBUILD RUN cd /usr/src/app \
      && mvn assembly:assembly

ONBUILD RUN rm /app/application.jar && \
        cp TopicConsumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar /app/application.jar