FROM maven:3.3.9-jdk-8-onbuild

MAINTAINER Kostiantyn Shchepanovskyi <schepanovsky@gmail.com>

ENTRYPOINT ["./wrapper"]
