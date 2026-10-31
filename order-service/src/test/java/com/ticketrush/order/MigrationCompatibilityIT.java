package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class MigrationCompatibilityIT {

    @Test
    void v2MigratesExistingPaymentFailures() throws Exception {
        try (var postgres = new PostgreSQLContainer("postgres:18.6")) {
            postgres.start();
            Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .schemas("orders").target("1").load().migrate();
            UUID id = UUID.randomUUID();
            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 var insert = connection.prepareStatement(
                         "insert into orders.orders (order_id, status) values (?, 'PAYMENT_FAILED')")) {
                insert.setObject(1, id);
                assertThat(insert.executeUpdate()).isEqualTo(1);
            }

            Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .schemas("orders").load().migrate();

            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 var query = connection.prepareStatement("select status from orders.orders where order_id = ?")) {
                query.setObject(1, id);
                try (var rows = query.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getString(1)).isEqualTo("FAILED");
                }
            }
        }
    }

    @Test
    void v2AllowsLegacyPaymentFailureWrites() throws Exception {
        try (var postgres = new PostgreSQLContainer("postgres:18.6")) {
            postgres.start();
            Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .schemas("orders").load().migrate();

            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 var insert = connection.prepareStatement(
                         "insert into orders.orders (order_id, status) values (?, 'PAYMENT_FAILED')")) {
                UUID id = UUID.randomUUID();
                insert.setObject(1, id);
                assertThat(insert.executeUpdate()).isEqualTo(1);
                try (var query = connection.prepareStatement("select status from orders.orders where order_id = ?")) {
                    query.setObject(1, id);
                    try (var rows = query.executeQuery()) {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getString(1)).isEqualTo("PAYMENT_FAILED");
                    }
                }
            }
        }
    }
}
