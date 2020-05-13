FROM openjdk:8u181-jdk-alpine3.8

MAINTAINER Naren <nanichowdary.ravilla@gmail.com>

RUN mkdir /apps
COPY src/main/resources /apps
COPY target/neo4j-graphql-streams-demo-0.0.1.jar /apps/streams-0.0.1.jar

ENTRYPOINT exec java -jar /apps/streams-0.0.1.jar --spring.profiles.active=application