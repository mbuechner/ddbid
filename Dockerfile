FROM maven:3-openjdk-11-slim AS MAVEN_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM openjdk:11-jre-slim
ENV DDBID.PORT=8080
RUN mkdir /home/ddbid
COPY --from=MAVEN_CHAIN /tmp/target/ddbid.jar /home/ddbid/ddbid.jar
WORKDIR /home/ddbid/
CMD ["java", "-Xms512M", "-Xmx1G", "-jar", "ddbid.jar"]

EXPOSE 8080
