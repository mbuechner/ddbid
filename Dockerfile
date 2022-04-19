FROM maven:3-openjdk-11-slim AS MAVEN_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM debian:bullseye-slim
ENV TZ=Europe/Berlin
ENV DDBID.PORT=8080
ENV XDG_CONFIG_HOME=/tmp
RUN apt-get -y update && apt-get -y install openjdk-11-jre nano && mkdir /home/ddbid
RUN apt-get -y install wget unzip && \
     wget "https://github.com/duckdb/duckdb/releases/download/v0.3.2/duckdb_cli-linux-amd64.zip" -O /tmp/temp.zip && \
     unzip /tmp/temp.zip -d /usr/bin/ && \
     chmod 755 /usr/bin/duckdb && \
     rm /tmp/temp.zip && \
     apt-get -y remove wget unzip
COPY --from=MAVEN_CHAIN /tmp/target/ddbid.jar /home/ddbid/ddbid.jar
WORKDIR /home/ddbid/
CMD ["java", "-Xms512M", "-Xmx1G", "-Xss512k", "-XX:MaxDirectMemorySize=2G","-XX:+UseShenandoahGC", "-XX:+UnlockExperimentalVMOptions", "-XX:ShenandoahUncommitDelay=1000", "-XX:ShenandoahGuaranteedGCInterval=10000", "-jar", "ddbid.jar"]

EXPOSE 8080
