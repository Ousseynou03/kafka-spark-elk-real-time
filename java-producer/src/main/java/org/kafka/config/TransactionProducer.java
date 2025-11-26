package org.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class TransactionProducer implements CommandLineRunner {

    private static final String KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092";
    private static final String TOPIC_NAME = "financials_transactions";
    private static final int NUM_PARTITIONS = 5;
    private static final short REPLICATION = 3;

    private final ObjectMapper mapper = new ObjectMapper();

    /** ========= CONVERSION DE TON PYTHON ========== */

    @Override
    public void run(String... args) throws Exception {
        createTopicIfNeeded();
        startParallelProducers(3);

        // Bloque le thread principal pour que les daemons continuent de tourner
        Thread.currentThread().join();
    }


    /** --- Création du topic (équivalent Python AdminClient) --- */
    private void createTopicIfNeeded() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get();

            if (!existingTopics.contains(TOPIC_NAME)) {
                NewTopic newTopic = new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("✔ Topic créé : " + TOPIC_NAME);
            } else {
                System.out.println("✔ Topic déjà existant : " + TOPIC_NAME);
            }
        }
    }

    /** --- Démarre plusieurs threads Producers --- */
    private void startParallelProducers(int numThreads) {
        for (int i = 0; i < numThreads; i++) {
            int threadId = i;

            Thread t = new Thread(() -> produceLoop(threadId));
            t.setDaemon(true);
            t.start();
        }
    }

    /** --- Boucle infinie pour produire des messages (comme Python) --- */
    private void produceLoop(int threadId) {
        KafkaProducer<String, String> producer = createKafkaProducer();

        while (true) {
            try {
                Map<String, Object> transaction = generateTransaction();
                String key = transaction.get("userId").toString();
                String value = mapper.writeValueAsString(transaction);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC_NAME, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null)
                        System.out.println("Delivery failed: " + key);
                    else
                        System.out.println("✔ Thread " + threadId + " produced: " + value);
                });

                producer.flush();
                Thread.sleep(500);

            } catch (Exception e) {
                System.out.println("Error producing: " + e.getMessage());
            }
        }
    }

    /** --- Config producer (équivalent Python) --- */
    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BROKERS);
        props.put("acks", "1");
        props.put("compression.type", "gzip");
        props.put("linger.ms", "10");
        props.put("batch.size", "1000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    /** --- Génération des transactions (copie exacte Python) --- */
    private Map<String, Object> generateTransaction() {
        Random r = new Random();

        Map<String, Object> map = new HashMap<>();
        map.put("transactionId", UUID.randomUUID().toString());
        map.put("userId", "user-" + (1 + r.nextInt(100)));
        map.put("amount", 50_000 + (r.nextDouble() * 100_000));
        map.put("transactionTime", System.currentTimeMillis() / 1000);
        map.put("merchantId", pick(r, "merchant_1", "merchant_2", "merchant_3"));
        map.put("transactionType", pick(r, "purchase", "refund"));
        map.put("location", "location_" + (1 + r.nextInt(50)));
        map.put("payment_method", pick(r, "credit_card", "paypal", "bank_transfer"));
        map.put("isinternational", r.nextBoolean());
        map.put("currency", pick(r, "USD", "EUR", "FCFA"));

        return map;
    }

    private String pick(Random r, String... values) {
        return values[r.nextInt(values.length)];
    }
}
