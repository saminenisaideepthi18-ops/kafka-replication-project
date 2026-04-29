# Kafka MirrorMaker 2 — Enhanced Fault Tolerance

A production-ready, fault-tolerant Apache Kafka replication pipeline using a **modified MirrorMaker 2** with automatic log truncation detection and topic reset recovery.

---

## 🔗 Repository Links

- **Kafka Fork**: [saminenisaideepthi18-ops/kafka](https://github.com/saminenisaideepthi18-ops/kafka)
- **Pull Request**: [MirrorMaker 2 Fault-Tolerance Enhancement #2](https://github.com/saminenisaideepthi18-ops/kafka/pull/2)

---

## 🏗️ Architecture

```
┌─────────────────────────┐        ┌──────────────────────────┐
│   Primary Cluster       │        │   DR / Standby Cluster   │
│   kafka-primary:9092    │        │   kafka-dr:9093           │
│                         │        │                          │
│   Topic: commit-log     │──MM2──▶│  Topic: primary.commit-log│
│   (WAL, 1 partition)    │        │  (replicated, 1 partition)│
└─────────────────────────┘        └──────────────────────────┘
          ▲
          │
┌─────────────────┐
│ Commit Log      │
│ Producer (CLI)  │
│ --count N msgs  │
└─────────────────┘
```

---

## 🚀 Quick Start (How to Run)

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed
- Git Bash (for Windows users)

### 2. Execution
To run the full automated validation suite (all 3 scenarios), simply execute:

```bash
# Clone and enter the project
git clone https://github.com/saminenisaideepthi18-ops/kafka-replication-project.git
cd kafka-replication-project

# Run the automation script
bash run_challenge.sh
```

### 3. Manual Monitoring
```bash
# Watch MirrorMaker 2 logs
docker logs mirrormaker2 -f

# Check DR message count
docker exec kafka-dr /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic primary.commit-log --time -1
```

---

## 📋 Log Analysis Guide

| Scenario | Log Pattern | Meaning |
|---|---|---|
| Normal | `Polled N records from commit-log` | Healthy replication |
| Truncation | `[TRUNCATION DETECTED]` / `KafkaException` | Fail-fast triggered ✅ |
| Topic Reset | `[TOPIC RESET DETECTED]` / `Seeking to beginning` | Auto-recovery triggered ✅ |

---

## 🔧 Design Rationale — MirrorMaker 2 Modifications

### Modified File
`connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`

### Task 2: Log Truncation Detection (Fail-Fast)
- Maintain a `Map<TopicPartition, Long> expectedNextOffsets` tracking the next expected offset per partition.
- If `firstOffset > expectedNextOffset` → a gap exists → Data loss detected.
- Throw a `KafkaException` to **fail fast** rather than silently skip records.

### Task 3: Graceful Topic Reset Handling
- Wrap the `poll()` call in a try-catch for `OffsetOutOfRangeException`.
- On detection: log the event, clear the `expectedNextOffsets` map, and seek the consumer to **offset 0**.
- This allows MM2 to resume normally after a topic recreate without manual intervention.

---

## 📁 Project Structure

```
kafka-replication-project/
├── docker-compose.yml          # Full environment setup
├── mm2.properties              # MirrorMaker 2 configuration
├── run_challenge.sh            # Automated test scenarios
├── README.md                   # This file
└── producer/
    ├── ProducerApp.java        # CLI producer with --count N support
    └── Dockerfile              # Containerized producer
```

---

## 📸 Screenshots & Proof of Execution

### 1. Scenario 1: Normal Replication Success
![Scenario 1 Results](screenshots/scenario1.png)

### 2. Scenario 2: Log Truncation Detection (Fail-Fast)
![Scenario 2 Truncation Detected](screenshots/scenario2.png)

### 3. Scenario 3: Topic Reset & Auto-Recovery
![Scenario 3 Auto-Recovery Success](screenshots/scenario3.png)

---

## 🛑 Teardown

```bash
docker-compose down -v
```
