# syntax=docker/dockerfile:1.7
FROM maven:3-eclipse-temurin-25 AS build

WORKDIR /workspace
COPY pom.xml .
# Warm project dependencies without traversing plugin/report graphs. The broader
# dependency:go-offline goal can trip over stale npm WebJar version ranges.
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests dependency:resolve
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

FROM eclipse-temurin:25-jre-alpine

ENV TZ=Europe/Berlin

RUN addgroup -S ddbid \
    && adduser -S -G ddbid ddbid \
    && mkdir -p /app/data/dumps/item /app/data/dumps/person /app/data/dumps/organization \
    && chown -R ddbid:ddbid /app

WORKDIR /app
COPY --from=build --chown=ddbid:ddbid /workspace/target/ddbid.jar /app/ddbid.jar

USER ddbid
VOLUME ["/app/data"]
EXPOSE 8080

ENTRYPOINT ["java", "-Xms512M", "-Xmx1G", "-jar", "/app/ddbid.jar"]
