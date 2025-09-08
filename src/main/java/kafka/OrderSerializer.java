package kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import model.Order;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer implements Serializer<Order> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderSerializer() {
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(String topic, Order data) {
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Помилка серіалізації Order: " + e.getMessage(), e);
        }
    }
}
