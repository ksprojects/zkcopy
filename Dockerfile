FROM maven:3.3.9-jdk-8

MAINTAINER Kostiantyn Shchepanovskyi <schepanovsky@gmail.com>

ADD . /code
WORKDIR /code
RUN mvn install -DskipITs

ENTRYPOINT ["./wrapper"]
