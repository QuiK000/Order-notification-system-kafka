package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import model.Order;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class OrderDeserializer implements Deserializer<Order> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderDeserializer() {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public Order deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, Order.class);
        } catch (Exception e) {
            throw new SerializationException("Помилка: " + e.getMessage());
        }
    }
}
