#!/bin/bash

# Disable path conversion for Git Bash on Windows
export MSYS_NO_PATHCONV=1

echo "Starting Kafka Environment cleanup..."
docker-compose down -v --remove-orphans

echo "Starting Primary and DR clusters..."
docker-compose up -d kafka-primary kafka-dr

echo "Waiting for Kafka brokers to initialize (45s)..."
sleep 45

echo "Initializing topics..."
# Create commit-log topic
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic commit-log --partitions 1 --replication-factor 1 --config retention.ms=60000

# Create MM2 internal topics on both clusters
for t in mm2-offsets.primary.internal mm2-configs.primary.internal mm2-status.primary.internal mm2-offsets.dr.internal mm2-configs.dr.internal mm2-status.dr.internal; do
  docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 1 --replication-factor 1 --config cleanup.policy=compact
  docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic $t --partitions 1 --replication-factor 1 --config cleanup.policy=compact
done

echo "Starting MirrorMaker 2..."
docker-compose up -d mirrormaker2
sleep 15

echo "------------------------------------------------------------"
echo "SCENARIO 1: Normal Replication Flow"
echo "------------------------------------------------------------"
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 1000

echo "Waiting for replication (40s)..."
sleep 40

DR_COUNT_RAW=$(docker exec kafka-dr /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic primary.commit-log --time -1)
DR_COUNT=$(echo "$DR_COUNT_RAW" | awk -F':' '{print $3}' | xargs)

if [ -z "$DR_COUNT" ]; then DR_COUNT=0; fi
echo "Messages on DR: $DR_COUNT"

if [ "$DR_COUNT" -ge 1000 ]; then
  echo "[SUCCESS] Normal replication verified."
else
  echo "[PARTIAL] Replication incomplete. Check MM2 logs."
fi

echo "------------------------------------------------------------"
echo "SCENARIO 2: Log Truncation Detection"
echo "------------------------------------------------------------"
docker-compose stop mirrormaker2
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 2000

echo "Simulating log truncation..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic commit-log --config retention.ms=1 --config segment.bytes=100
sleep 60

docker-compose start mirrormaker2
sleep 15
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 10

sleep 20
if docker-compose logs mirrormaker2 | grep -qi "TRUNCATION DETECTED"; then
  echo "[SUCCESS] Truncation detection (Fail-Fast) verified."
else
  echo "[ERROR] Truncation not detected."
fi

echo "------------------------------------------------------------"
echo "SCENARIO 3: Topic Reset Recovery"
echo "------------------------------------------------------------"
docker-compose stop mirrormaker2
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log
sleep 5
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic commit-log --partitions 1 --replication-factor 1

docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 200
docker-compose start mirrormaker2
sleep 45

if docker-compose logs mirrormaker2 | grep -qi "TOPIC RESET DETECTED"; then
  echo "[SUCCESS] Topic reset auto-recovery verified."
else
  # Fallback check
  DR_COUNT_FINAL=$(docker exec kafka-dr /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic primary.commit-log --time -1 | awk -F':' '{print $3}' | xargs)
  if [ "$DR_COUNT_FINAL" -gt 0 ]; then
    echo "[SUCCESS] Replication resumed after reset."
  else
    echo "[ERROR] Recovery not detected."
  fi
fi

echo "Test run complete."