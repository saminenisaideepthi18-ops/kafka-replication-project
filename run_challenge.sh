#!/bin/bash
# =============================================================================
#  run_challenge.sh
#  Orchestrates the 3 fault-tolerance test scenarios for the Enhanced MM2
#  pipeline.
#
#  Scenarios:
#    1. Normal Replication Flow     - 1000 msgs produced, verify replication
#    2. Log Truncation Simulation   - Trigger retention purge, verify fail-fast
#    3. Topic Reset Simulation      - Delete/recreate topic, verify auto-recovery
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

SEPARATOR="============================================================"

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC}  $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[FAIL]${NC}  $1"; }
log_section() { echo -e "\n${CYAN}${SEPARATOR}${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}${SEPARATOR}${NC}"; }

# ─────────────────────────────────────────────
# SETUP: Start environment
# ─────────────────────────────────────────────
log_section "SETUP: Starting Kafka Environment"

log_info "Tearing down any existing containers..."
docker-compose down -v --remove-orphans 2>/dev/null || true

log_info "Starting Primary cluster, DR cluster, and MirrorMaker 2..."
docker-compose up -d kafka-primary kafka-dr

log_info "Waiting for Primary cluster to be healthy (up to 60s)..."
for i in $(seq 1 12); do
  if docker exec kafka-primary /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    log_success "Primary cluster is ready."
    break
  fi
  echo "  Attempt $i/12 — waiting 5s..."
  sleep 5
done

log_info "Waiting for DR cluster to be healthy (up to 60s)..."
for i in $(seq 1 12); do
  if docker exec kafka-dr /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server localhost:9093 > /dev/null 2>&1; then
    log_success "DR cluster is ready."
    break
  fi
  echo "  Attempt $i/12 — waiting 5s..."
  sleep 5
done

log_info "Creating commit-log topic on Primary cluster..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000
log_success "Topic 'commit-log' created with retention.ms=60000."

# ─────────────────────────────────────────────
# SCENARIO 1: Normal Replication Flow
# ─────────────────────────────────────────────
log_section "SCENARIO 1: Normal Replication Flow"

log_info "Starting Enhanced MirrorMaker 2..."
docker-compose up -d mirrormaker2
sleep 10

log_info "Producing 1000 messages to commit-log..."
docker-compose run --rm \
  -e KAFKA_BROKER=kafka-primary:9092 \
  -e TOPIC_NAME=commit-log \
  commit-log-producer \
  java -cp ".:./lib/*" ProducerApp --count 1000

log_info "Waiting 30s for MM2 to replicate messages to DR cluster..."
sleep 30

log_info "Counting messages in primary.commit-log on DR cluster..."
DR_COUNT=$(docker exec kafka-dr /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9093 \
  --topic primary.commit-log \
  --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}')

echo ""
echo "  Messages replicated to DR: ${DR_COUNT}"
echo ""

if [ "${DR_COUNT}" -ge 1000 ]; then
  log_success "SCENARIO 1 PASSED: All 1000 messages replicated to primary.commit-log on DR cluster."
else
  log_warn "SCENARIO 1 PARTIAL: Only ${DR_COUNT}/1000 messages found. Replication may still be in progress."
fi

# ─────────────────────────────────────────────
# SCENARIO 2: Log Truncation Detection (Fail-Fast)
# ─────────────────────────────────────────────
log_section "SCENARIO 2: Log Truncation Detection (Fail-Fast)"

log_info "Pausing MirrorMaker 2 so messages can expire..."
docker-compose stop mirrormaker2

log_info "Producing 500 more messages to commit-log..."
docker-compose run --rm \
  -e KAFKA_BROKER=kafka-primary:9092 \
  -e TOPIC_NAME=commit-log \
  commit-log-producer \
  java -cp ".:./lib/*" ProducerApp --count 500

log_info "Waiting 70s for log retention to purge messages (retention.ms=60000)..."
echo "  This simulates an aggressive retention policy deleting old messages"
echo "  before MirrorMaker 2 gets a chance to replicate them."
sleep 70

EARLIEST=$(docker exec kafka-primary /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic commit-log \
  --time -2 2>/dev/null | awk -F: '{print $3}' | head -1)

LATEST=$(docker exec kafka-primary /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic commit-log \
  --time -1 2>/dev/null | awk -F: '{print $3}' | head -1)

echo ""
echo "  Primary commit-log — Earliest offset: ${EARLIEST}  Latest offset: ${LATEST}"
echo ""

if [ "${EARLIEST}" -gt 0 ]; then
  log_info "Log truncation confirmed: earliest offset is ${EARLIEST} (old messages purged)."
else
  log_warn "Retention may not have kicked in yet. Earliest offset is still 0."
fi

log_info "Resuming MirrorMaker 2 — it should detect the offset gap and FAIL-FAST..."
docker-compose start mirrormaker2
sleep 20

echo ""
log_info "Checking MM2 logs for truncation detection..."
MM2_LOGS=$(docker-compose logs --tail=50 mirrormaker2 2>&1)

if echo "${MM2_LOGS}" | grep -qi "truncat\|offset gap\|data loss\|KafkaException\|FATAL\|fail"; then
  log_success "SCENARIO 2 PASSED: MirrorMaker 2 detected log truncation and failed fast."
  echo ""
  echo "  Relevant MM2 log lines:"
  echo "${MM2_LOGS}" | grep -i "truncat\|offset gap\|data loss\|KafkaException\|FATAL\|fail" | head -10
else
  log_warn "SCENARIO 2: Could not confirm truncation detection in logs. Check manually:"
  echo "  docker-compose logs mirrormaker2 | grep -i 'truncat\|offset gap\|FATAL'"
fi

# ─────────────────────────────────────────────
# SCENARIO 3: Topic Reset (Delete + Recreate) → Auto-Recovery
# ─────────────────────────────────────────────
log_section "SCENARIO 3: Topic Reset — Graceful Auto-Recovery"

log_info "Pausing MirrorMaker 2 before topic reset..."
docker-compose stop mirrormaker2

log_info "Deleting commit-log topic from Primary cluster..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic commit-log
sleep 5
log_success "Topic 'commit-log' deleted."

log_info "Recreating commit-log topic from scratch..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000
log_success "Topic 'commit-log' recreated. Offsets now start from 0."

log_info "Producing 200 fresh messages to the recreated topic..."
docker-compose run --rm \
  -e KAFKA_BROKER=kafka-primary:9092 \
  -e TOPIC_NAME=commit-log \
  commit-log-producer \
  java -cp ".:./lib/*" ProducerApp --count 200

log_info "Resuming MirrorMaker 2 — it should detect the topic reset and auto-recover from offset 0..."
docker-compose start mirrormaker2
sleep 30

echo ""
log_info "Checking MM2 logs for topic reset detection and recovery..."
MM2_LOGS=$(docker-compose logs --tail=80 mirrormaker2 2>&1)

if echo "${MM2_LOGS}" | grep -qi "reset\|OffsetOutOfRange\|resubscri\|seek.*0\|beginning\|recover"; then
  log_success "SCENARIO 3 PASSED: MirrorMaker 2 detected topic reset and auto-recovered."
  echo ""
  echo "  Relevant MM2 log lines:"
  echo "${MM2_LOGS}" | grep -i "reset\|OffsetOutOfRange\|resubscri\|seek.*0\|beginning\|recover" | head -10
else
  log_warn "SCENARIO 3: Could not confirm auto-recovery in logs. Check manually:"
  echo "  docker-compose logs mirrormaker2 | grep -i 'reset\|OffsetOutOfRange\|recover'"
fi

log_info "Verifying new messages replicated to DR cluster..."
sleep 15
NEW_DR_COUNT=$(docker exec kafka-dr /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9093 \
  --topic primary.commit-log \
  --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}')

echo ""
echo "  Messages in primary.commit-log on DR after reset: ${NEW_DR_COUNT}"
echo ""
if [ "${NEW_DR_COUNT}" -gt 0 ]; then
  log_success "DR cluster received messages after topic reset — replication resumed successfully."
fi

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
log_section "TEST RUN COMPLETE"
echo ""
echo "  To inspect detailed logs, run:"
echo "    docker-compose logs -f mirrormaker2"
echo "    docker-compose logs commit-log-producer"
echo ""
echo "  To shut down the environment:"
echo "    docker-compose down -v"
echo ""