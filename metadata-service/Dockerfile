FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml ./
COPY queue-core/pom.xml queue-core/pom.xml
COPY metadata-service/pom.xml metadata-service/pom.xml
COPY queue-node/pom.xml queue-node/pom.xml
COPY queue-benchmarks/pom.xml queue-benchmarks/pom.xml
COPY queue-core/src queue-core/src
COPY metadata-service/src metadata-service/src
RUN mvn -pl metadata-service -am -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build \
    /workspace/metadata-service/target/metadata-service-0.23.0-SNAPSHOT.jar \
    metadata-service.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/metadata-service.jar"]
