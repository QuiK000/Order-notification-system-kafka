package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Builder
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Order {
    private String id = UUID.randomUUID().toString();
    private String customerId = UUID.randomUUID().toString();
    private LocalDateTime orderDate = LocalDateTime.now();
    private OrderStatus status = OrderStatus.PENDING;
    private String productId;
    private BigDecimal totalAmount;
}
