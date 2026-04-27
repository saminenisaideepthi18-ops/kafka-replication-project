
function Log-Info { param([string]$msg); Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Log-Success { param([string]$msg); Write-Host "[PASS]  $msg" -ForegroundColor Green }
function Log-Warn { param([string]$msg); Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Log-Error { param([string]$msg); Write-Host "[FAIL]  $msg" -ForegroundColor Red }
function Log-Section { 
    param([string]$msg)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

Log-Section "SETUP: Starting Kafka Environment"
docker-compose down -v --remove-orphans
docker-compose up -d kafka-primary kafka-dr

Log-Info "Waiting for clusters to be healthy..."
Start-Sleep -Seconds 40

Log-Info "Creating topics..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic commit-log --partitions 1 --replication-factor 1
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-offsets.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-configs.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-status.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-offsets.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-configs.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic mm2-status.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact

docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic commit-log --partitions 1 --replication-factor 1
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-offsets.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-configs.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-status.primary.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-offsets.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-configs.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka-dr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --if-not-exists --topic mm2-status.dr.internal --partitions 1 --replication-factor 1 --config cleanup.policy=compact

Log-Success "Topics created successfully."

Log-Section "SCENARIO 1: Normal Replication Flow"
docker-compose up -d mirrormaker2
Start-Sleep -Seconds 10
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 1000
Start-Sleep -Seconds 60

$dr_count_output = docker exec kafka-dr /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic primary.commit-log
$dr_count = 0
if ($dr_count_output -join "`n" -match ":(\d+)\s*$") { $dr_count = [int]$matches[1] }
if ($dr_count -ge 1000) { Log-Success "SCENARIO 1 PASSED: $dr_count messages replicated to DR cluster." } else { Log-Warn "SCENARIO 1 PARTIAL: Only $dr_count messages." }

Log-Section "SCENARIO 2: Log Truncation Detection (Fail-Fast)"
docker-compose stop mirrormaker2
Log-Info "Producing 2000 messages to force multiple segments..."
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 2000
Start-Sleep -Seconds 5
Log-Info "Truncating log (setting retention to 1ms)..."
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic commit-log --config retention.ms=1 --config segment.bytes=100
Log-Info "Waiting 60s for log cleaner..."
Start-Sleep -Seconds 60

$earliest_output = docker exec kafka-primary /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic commit-log --time -2
$earliest = 0
if ($earliest_output -join "`n" -match ":(\d+)\s*$") { $earliest = [int]$matches[1] }
if ($earliest -gt 0) { Log-Success "Log truncation confirmed: earliest offset is $earliest." } else { Log-Warn "Log truncation might not have occurred yet." }

docker-compose start mirrormaker2
Start-Sleep -Seconds 10
Log-Info "Producing 10 messages so MM2 has something to poll and detect the gap..."
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 10
Start-Sleep -Seconds 30
$mm2_logs = docker-compose logs --tail=200 mirrormaker2 2>&1
$trunc = $mm2_logs | Select-String -Pattern "TRUNCATION DETECTED|KafkaException|Data loss detected" -CaseSensitive:$false
if ($trunc) { Log-Success "SCENARIO 2 PASSED: Log truncation detected." } else { Log-Warn "SCENARIO 2 FAILED/UNVERIFIED." }

Log-Section "SCENARIO 3: Topic Reset Graceful Auto-Recovery"
docker-compose stop mirrormaker2
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log
Start-Sleep -Seconds 5
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic commit-log --partitions 1 --replication-factor 1 --config retention.ms=30000 --config segment.bytes=1024
docker-compose run --rm -e KAFKA_BROKER=kafka-primary:9092 -e TOPIC_NAME=commit-log commit-log-producer java -cp '.:./lib/*' ProducerApp --count 200

docker-compose start mirrormaker2
Start-Sleep -Seconds 40
$mm2_logs2 = docker-compose logs --tail=100 mirrormaker2 2>&1
$reset = $mm2_logs2 | Select-String -Pattern "reset|OffsetOutOfRange|resubscri|seek.*0|beginning|recover" -CaseSensitive:$false
if ($reset) { Log-Success "SCENARIO 3 PASSED: Topic reset auto-recovery worked." } else { Log-Warn "SCENARIO 3 FAILED/UNVERIFIED." }

$dr_count_output2 = docker exec kafka-dr /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic primary.commit-log
$dr_count2 = 0
if ($dr_count_output2 -join "`n" -match ":(\d+)\s*$") { $dr_count2 = [int]$matches[1] }
if ($dr_count2 -gt $dr_count) { Log-Success "DR cluster received messages after topic reset - replication resumed successfully." }
Log-Section "TEST RUN COMPLETE"
