FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /app

COPY ProducerApp.java .

# Download kafka-clients and required dependencies
RUN mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:3.7.0
RUN mvn dependency:get -Dartifact=org.slf4j:slf4j-api:1.7.36
RUN mvn dependency:get -Dartifact=org.slf4j:slf4j-simple:1.7.36

# Compile with all dependencies on classpath
RUN javac \
    -cp "/root/.m2/repository/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar:/root/.m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar" \
    ProducerApp.java

# ---- Runtime Stage ----
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /app/ProducerApp.class .
COPY --from=builder /root/.m2/repository/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar ./lib/
COPY --from=builder /root/.m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar ./lib/
COPY --from=builder /root/.m2/repository/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar ./lib/
COPY --from=builder /root/.m2/repository/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar ./lib/

# Default: produce 1000 messages. Override via docker-compose command: ["--count", "500"]
CMD ["java", "-cp", ".:./lib/*", "ProducerApp", "--count", "1000"]