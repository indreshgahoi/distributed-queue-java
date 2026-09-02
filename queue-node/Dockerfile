FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml ./
COPY queue-core/pom.xml queue-core/pom.xml
COPY metadata-service/pom.xml metadata-service/pom.xml
COPY queue-node/pom.xml queue-node/pom.xml
COPY queue-benchmarks/pom.xml queue-benchmarks/pom.xml
COPY queue-core/src queue-core/src
COPY queue-node/src queue-node/src
RUN mvn -pl queue-node -am -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build \
    /workspace/queue-node/target/queue-node-0.24.0-SNAPSHOT-exec.jar \
    queue-node.jar

ENTRYPOINT ["java", "-jar", "/app/queue-node.jar"]
