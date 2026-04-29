import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Commit Log Producer
 *
 * Generates JSON events to the primary cluster's commit-log topic.
 * Usage: java ProducerApp --count N
 *
 * Each event follows the schema:
 * {
 * "event_id": "<uuid>",
 * "timestamp": <epoch_seconds>,
 * "op_type": "<INSERT|UPDATE|DELETE>",
 * "key": "<doc:<hex>>",
 * "value": { "status": "<string>" }
 * }
 */
public class ProducerApp {

    // Possible operation types for realistic simulation
    private static final String[] OP_TYPES = { "INSERT", "UPDATE", "DELETE" };
    private static final String[] STATUSES = { "active", "archived", "pending", "deleted", "published" };

    public static void main(String[] args) {
        // --- Parse --count N from CLI arguments ---
        int count = parseCount(args);

        String brokerUrl = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
        String topic = System.getenv().getOrDefault("TOPIC_NAME", "commit-log");

        System.out.println("   Commit Log Producer Starting Up");
        System.out.println("Target Broker : " + brokerUrl);
        System.out.println("Target Topic  : " + topic);
        System.out.println("Message Count : " + count);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Ensure all replicas acknowledge for durability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String eventJson = buildEventJson(i);
                // Use the doc key as the Kafka message key for partitioning
                String kafkaKey = "doc:" + String.format("%04x", i);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, kafkaKey, eventJson);

                // Send synchronously to guarantee ordering (WAL semantics)
                producer.send(record).get();

                if (i % 100 == 0 || i == count) {
                    System.out.println("[" + i + "/" + count + "] Sent: " + kafkaKey);
                }
            }
            System.out.println("All " + count + " events produced successfully!");
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("[ERROR] Failed to produce messages: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Parses --count N from CLI args. Defaults to 1000 if not provided.
     */
    private static int parseCount(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--count".equals(args[i])) {
                try {
                    int val = Integer.parseInt(args[i + 1]);
                    if (val <= 0) {
                        System.err.println("[ERROR] --count must be a positive integer. Got: " + val);
                        System.exit(1);
                    }
                    return val;
                } catch (NumberFormatException e) {
                    System.err.println("[ERROR] Invalid --count value: " + args[i + 1]);
                    System.exit(1);
                }
            }
        }
        // Default count if --count not provided
        System.out.println("[INFO] --count not specified. Defaulting to 1000.");
        return 1000;
    }

    /**
     * Builds a JSON event matching the required schema.
     */
    private static String buildEventJson(int index) {
        String eventId = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis() / 1000L; // epoch seconds
        String opType = OP_TYPES[index % OP_TYPES.length];
        String key = "doc:" + String.format("%04x", index);
        String status = STATUSES[index % STATUSES.length];

        return "{"
                + "\"event_id\":\"" + eventId + "\","
                + "\"timestamp\":" + timestamp + ","
                + "\"op_type\":\"" + opType + "\","
                + "\"key\":\"" + key + "\","
                + "\"value\":{\"status\":\"" + status + "\"}"
                + "}";
    }
}
