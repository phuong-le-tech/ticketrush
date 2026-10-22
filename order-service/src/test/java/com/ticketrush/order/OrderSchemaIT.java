package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Schema orders.orders tel que valide par l'owner, sur un vrai Postgres, via les migrations
 * Flyway du module (classpath:db/migration). Pas de Spring ni de Kafka ici.
 */
class OrderSchemaIT {

    @BeforeAll
    static void migrate() {
        Flyway.configure()
                .dataSource(OrderTestInfra.POSTGRES.getJdbcUrl(), OrderTestInfra.POSTGRES.getUsername(),
                        OrderTestInfra.POSTGRES.getPassword())
                .schemas("orders")
                .load()
                .migrate();
    }

    private static void insert(UUID id, String status, Integer quantity) throws SQLException {
        try (Connection c = OrderTestInfra.connection();
             PreparedStatement ps = c.prepareStatement(
                     "insert into orders.orders (order_id, status, quantity) values (?, ?, ?)")) {
            ps.setObject(1, id);
            ps.setString(2, status);
            ps.setObject(3, quantity, Types.INTEGER);
            ps.executeUpdate();
        }
    }

    @Test
    void rowWithOnlyOrderIdAndStatusIsAccepted() throws Exception {
        UUID id = UUID.randomUUID();

        insert(id, "CONFIRMED", null);

        OrderTestInfra.Row row = OrderTestInfra.rows(id).get(0);
        assertThat(row.status()).isEqualTo("CONFIRMED");
        assertThat(row.idempotencyKey()).isNull();
        assertThat(row.eventId()).isNull();
        assertThat(row.customerId()).isNull();
        assertThat(row.quantity()).isNull();
    }

    @Test
    void unknownStatusIsRejectedByTheCheckConstraint() {
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "FOO", null))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }

    @Test
    void quantityAboveFourIsRejectedByTheCheckConstraint() {
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "PENDING", 5))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }
}
