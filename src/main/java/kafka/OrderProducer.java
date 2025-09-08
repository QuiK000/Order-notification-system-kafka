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
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class OrderProducer {
    private final KafkaProducer<String, Order> producer;
    private final String transactionalId;

    public OrderProducer(String bootstrapServers, String transactionalId) {
        this.transactionalId = transactionalId;
        this.producer = createProducer(bootstrapServers);
    }

    public void sendOrder(Order order) throws ExecutionException, InterruptedException {
        try {
            producer.beginTransaction();

            ProducerRecord<String, Order> record = new ProducerRecord<>(
                    "orders",
                    order.getId(),
                    order
            );

            var metadata = producer.send(record).get();
            System.out.printf(
                    "Повідомлення відправлено успішно: topic=%s, partition=%d, offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset()
            );

            producer.commitTransaction();
            System.out.println("Транзакція успішно завершена");
        } catch (Exception e) {
            producer.abortTransaction();
            System.err.println("Транзакція відкатана через помилку: " + e.getMessage());
            throw e;
        }
    }

    public void close() {
        producer.close();
    }

    private KafkaProducer<String, Order> createProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        properties.put(ACKS_CONFIG, "all");
        properties.put(RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(TRANSACTIONAL_ID_CONFIG, transactionalId);

        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        OrderProducer orderProducer = new OrderProducer("192.168.0.100:9092", "order-transactional-orderProducer");

        try {
            orderProducer.producer.initTransactions();

            Order testOrder = Order
                    .builder()
                    .id(UUID.randomUUID().toString())
                    .customerId(UUID.randomUUID().toString())
                    .status(OrderStatus.PENDING)
                    .productId(UUID.randomUUID().toString())
                    .totalAmount(BigDecimal.valueOf(350.30))
                    .build();

            orderProducer.sendOrder(testOrder);
        } catch (Exception e) {
            System.err.println("Помилка при відправці замовлення: " + e.getMessage());
        } finally {
            orderProducer.close();
        }
    }
}
