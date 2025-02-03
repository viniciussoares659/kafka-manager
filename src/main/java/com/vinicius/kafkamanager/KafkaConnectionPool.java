package com.vinicius.kafkamanager;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaConnectionPool {
    private static final Map<String, Admin> adminClients = new ConcurrentHashMap<>();
    private static final Map<String, KafkaProducer<Object, Object>> producers = new ConcurrentHashMap<>();
    private static final Map<String, KafkaConsumer<Object, Object>> consumers = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public static void startHealthCheck(long intervalSeconds) {
        scheduler.scheduleAtFixedRate(
                KafkaConnectionPool::checkConnectionHealth,
                intervalSeconds,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }

    public static void checkConnectionHealth() {
        adminClients.entrySet().removeIf(entry -> {
            try {
                entry.getValue().describeCluster().nodes().get(5, TimeUnit.SECONDS);
                return false; // Conexão boa, mantém no cache
            } catch (Exception e) {
                System.out.println("Removendo conexão inativa: " + entry.getKey());
                return true; // Remove conexão do cache
            }
        });
    }

    public static Admin getAdminClient(String bootstrapServers) {
        return adminClients.computeIfAbsent(bootstrapServers, servers ->
                Admin.create(Map.of(
                        "bootstrap.servers", servers,
                        "connections.max.idle.ms", "300000"
                ))
        );
    }

    public static KafkaProducer<Object, Object> getProducer(String bootstrapServers) {
        return producers.computeIfAbsent(bootstrapServers, servers ->
                new KafkaProducer<>(Map.of(
                        "bootstrap.servers", servers,
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "linger.ms", "10",
                        "batch.size", "16384"
                ))
        );
    }

    public static KafkaConsumer<Object, Object> getConsumer(String bootstrapServers, String groupId) {
        String key = bootstrapServers + "-" + groupId;
        return consumers.computeIfAbsent(key, k ->
                new KafkaConsumer<>(Map.of(
                        "bootstrap.servers", bootstrapServers,
                        "group.id", groupId,
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "enable.auto.commit", "false",
                        "auto.offset.reset", "earliest"
                ))
        );
    }

    public static void shutdown() {
        scheduler.shutdown();
        closeAll();
    }

    private static void closeAll() {
        adminClients.values().forEach(Admin::close);
        producers.values().forEach(KafkaProducer::close);
        consumers.values().forEach(KafkaConsumer::close);
        adminClients.clear();
        producers.clear();
        consumers.clear();
    }
}