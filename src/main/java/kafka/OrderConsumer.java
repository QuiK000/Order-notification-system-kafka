package kafka;

import model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class OrderConsumer {
    private final KafkaConsumer<String, Order> consumer;
    private static final String TOPIC = "orders";

    public OrderConsumer(String bootstrapServer, String groupId) {
        this.consumer = createConsumer(bootstrapServer, groupId);
        this.consumer.subscribe(Collections.singletonList(TOPIC));
    }

    public void consumeOrders() {
        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    System.out.println("Пусто...");
                    continue;
                }

                StreamSupport.stream(records.spliterator(), false)
                        .forEach(record -> {
                            System.out.println("Замовлення: ");
                            System.out.println(" Ключ: " + record.key());
                            System.out.println(" Дата: " + record.value());
                            System.out.println("======================");
                        });

                consumer.commitAsync();
            }
        } catch (Exception e) {
            System.err.println("Помилка: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private KafkaConsumer<String, Order> createConsumer(String bootstrapServer, String groupId) {
        var properties = new Properties();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(MAX_POLL_RECORDS_CONFIG, 10);

        return new KafkaConsumer<>(properties);
    }

    public void close() {
        consumer.close();
    }

    public static void main(String[] args) {
        OrderConsumer orderConsumer = new OrderConsumer("192.168.0.100:9092", "order-consumer-group");
        Runtime.getRuntime().addShutdownHook(new Thread(orderConsumer::close));

        try {
            orderConsumer.consumeOrders();
        } catch (Exception e) {
            System.err.println("Помилка: " + e.getMessage());
        }
    }
}
