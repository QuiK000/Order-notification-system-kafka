package kafka;

import model.Order;
import model.OrderStatus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class Producer {
    public static void main(String[] args) {
        var properties = new Properties();
        var testOrder = Order
                .builder()
                .id(UUID.randomUUID().toString())
                .customerId(UUID.randomUUID().toString())
                .status(OrderStatus.PENDING)
                .productId(UUID.randomUUID().toString())
                .totalAmount(BigDecimal.valueOf(350.23))
                .build();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
        properties.put(ACKS_CONFIG, "all");
        properties.put(RETRIES_CONFIG, 3);

        try (var producer = new KafkaProducer<String, Order>(properties)) {
            ProducerRecord<String, Order> record = new ProducerRecord<>("sandbox", testOrder.getId(), testOrder);
            var metadata = producer.send(record).get();
            System.out.println("Повідомлення відправлено успішно: " + metadata.topic());
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Помилка при відправці повідомлення: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
