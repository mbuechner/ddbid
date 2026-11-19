# syntax=docker/dockerfile:1.7
FROM maven:3-amazoncorretto-25 AS build

WORKDIR /workspace
COPY pom.xml .
# Warm project dependencies without traversing plugin/report graphs. The broader
# dependency:go-offline goal can trip over stale npm WebJar version ranges.
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp dependency:resolve
COPY src ./src
ARG SOURCE_DATE_EPOCH
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp \
    ${SOURCE_DATE_EPOCH:+"-Dproject.build.outputTimestamp=$SOURCE_DATE_EPOCH"} verify

FROM amazoncorretto:25

ENV TZ=Europe/Berlin \
    HOME=/app \
    XDG_CONFIG_HOME=/app/.config \
    SCHEDULER_ENABLED=false \
    JAVA_TOOL_OPTIONS="-Duser.home=/app"

# Preserve the Alpine image's IDs for files created in existing data volumes.
RUN ! getent passwd 100 \
    && ! getent group 101 \
    && printf '%s\n' 'ddbid:!:100:101:DDBid:/app:/sbin/nologin' >> /etc/passwd \
    && printf '%s\n' 'ddbid:!:101:' >> /etc/group \
    && mkdir -p /app/.config/jgit /app/data/dumps/item /app/data/dumps/person /app/data/dumps/organization \
    && chown -R ddbid:0 /app \
    && chmod -R g=u /app

WORKDIR /app
COPY --from=build --chown=ddbid:0 --chmod=0644 /workspace/target/ddbid.jar /app/ddbid.jar
COPY --from=build --chown=ddbid:0 --chmod=0644 /workspace/target/classes/META-INF/sbom/application.cdx.json /app/ddbid-sbom.json

USER ddbid
VOLUME ["/app/data"]
EXPOSE 8080

ENTRYPOINT ["java", "-Xms512M", "-Xmx1G", "-jar", "/app/ddbid.jar"]
