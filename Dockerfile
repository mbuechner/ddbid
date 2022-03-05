FROM maven:3-openjdk-11-slim AS MAVEN_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM debian:bullseye-slim
ENV DDBID.PORT=8080
RUN apt-get -y update && apt-get -y install openjdk-11-jre && mkdir /home/ddbid
COPY --from=MAVEN_CHAIN /tmp/target/ddbid.jar /home/ddbid/ddbid.jar
WORKDIR /home/ddbid/
CMD ["java", "-Xms512M", "-Xmx1G", "-Xss512k", "-XX:+UseShenandoahGC", "-XX:+UnlockExperimentalVMOptions", "-XX:ShenandoahUncommitDelay=1000", "-XX:ShenandoahGuaranteedGCInterval=10000", "-jar", "ddbid.jar"]

EXPOSE 8080
