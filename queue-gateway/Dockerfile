FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml ./
COPY queue-core/pom.xml queue-core/pom.xml
COPY metadata-service/pom.xml metadata-service/pom.xml
COPY queue-node/pom.xml queue-node/pom.xml
COPY queue-gateway/pom.xml queue-gateway/pom.xml
COPY queue-benchmarks/pom.xml queue-benchmarks/pom.xml
COPY queue-gateway/src queue-gateway/src
RUN mvn -pl queue-gateway -am -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build \
    /workspace/queue-gateway/target/queue-gateway-0.25.0-SNAPSHOT.jar \
    queue-gateway.jar

ENTRYPOINT ["java", "-jar", "/app/queue-gateway.jar"]
