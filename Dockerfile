FROM maven:3.8-jdk-11

MAINTAINER Kostiantyn Shchepanovskyi <schepanovsky@gmail.com>

ADD . /code
WORKDIR /code
RUN mvn install -DskipITs

ENTRYPOINT ["./wrapper"]
