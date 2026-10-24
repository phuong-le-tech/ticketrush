package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

/** Redis reel (Testcontainers), aucun mock: l'atomicite du script Lua est le risque central. */
class InventoryRepositoryIT {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);
    private static final Duration DEDUP_TTL = Duration.ofDays(7);

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private InventoryRepository repository;

    @BeforeAll
    static void startRedis() {
        REDIS.start();
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        factory.destroy();
        REDIS.stop();
    }

    @BeforeEach
    void cleanRedis() {
        redis.execute((RedisCallback<Object>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        repository = new InventoryRepository(redis, DEDUP_TTL);
    }

    private static String key(String eventId) {
        return "inventory:event:" + eventId;
    }

    /** Each call is a distinct order (fresh orderId), so the per-order dedupe never kicks in here. */
    private ReservationResult reserveNewOrder(String eventId, int quantity) {
        return repository.reserve(eventId, UUID.randomUUID(), quantity, Instant.now().plusSeconds(300));
    }

    private static void putEvent(String eventId, long stock, long priceCents, String currency) {
        redis.opsForHash().put(key(eventId), "stock", String.valueOf(stock));
        redis.opsForHash().put(key(eventId), "priceCents", String.valueOf(priceCents));
        redis.opsForHash().put(key(eventId), "currency", currency);
    }

    private static Object field(String eventId, String field) {
        return redis.opsForHash().get(key(eventId), field);
    }

    @Test
    void reserveDecrementsStockAndReturnsPriceAndCurrency() {
        putEvent("e1", 3, 12550, "EUR");

        ReservationResult result = reserveNewOrder("e1", 2);

        assertThat(result.reserved()).isTrue();
        assertThat(result.priceCents()).isEqualTo(12550L);
        assertThat(result.currency()).isEqualTo("EUR");
        assertThat(field("e1", "stock")).isEqualTo("1");
    }

    @Test
    void reserveIsAllOrNothingWhenStockIsInsufficient() {
        putEvent("e1", 3, 12550, "EUR");
        assertThat(reserveNewOrder("e1", 2).reserved()).isTrue();

        ReservationResult second = reserveNewOrder("e1", 2);

        assertThat(second.reserved()).isFalse();
        assertThat(field("e1", "stock")).isEqualTo("1");
    }

    @Test
    void reservingExactlyTheRemainingStockSucceedsThenStockNeverGoesNegative() {
        putEvent("e1", 2, 12550, "EUR");

        assertThat(reserveNewOrder("e1", 2).reserved()).isTrue();
        assertThat(field("e1", "stock")).isEqualTo("0");

        assertThat(reserveNewOrder("e1", 1).reserved()).isFalse();
        assertThat(field("e1", "stock")).isEqualTo("0");
    }

    private ReservationResult reserveOrder(String eventId, UUID orderId) {
        return repository.reserve(eventId, orderId, 1, Instant.now().plusSeconds(300));
    }

    @Test
    void unknownEventAC5StoresRejectionWithTtlAndCreatesNoEventKey() {
        UUID o1 = UUID.randomUUID();
        String orderKey = "inventory:order:" + o1;

        ReservationResult result = reserveOrder("ghost", o1);

        assertThat(result.reserved()).isFalse();
        assertThat(redis.keys("*")).containsExactly(orderKey);
        assertThat(redis.opsForHash().get(orderKey, "reserved")).isEqualTo("0");
        assertThat(redis.getExpire(orderKey)).isPositive().isLessThanOrEqualTo(604800L);
        assertThat(redis.hasKey(key("ghost"))).isFalse();
    }

    @Test
    void rejectedOrderAC5StaysRejectedAfterEventIsSeeded() {
        UUID o1 = UUID.randomUUID();
        reserveOrder("ghost", o1);
        putEvent("ghost", 5, 1000, "EUR");

        ReservationResult replay = reserveOrder("ghost", o1);

        assertThat(replay.reserved()).isFalse();
        assertThat(field("ghost", "stock")).isEqualTo("5");
    }

    @Test
    void freshOrderAC5CanStillReserveAfterRejectedOrderOnSeededEvent() {
        reserveOrder("ghost", UUID.randomUUID());
        putEvent("ghost", 5, 1000, "EUR");

        ReservationResult fresh = reserveOrder("ghost", UUID.randomUUID());

        assertThat(fresh.reserved()).isTrue();
        assertThat(field("ghost", "stock")).isEqualTo("4");
    }

    @Test
    void unknownEventReplayAC5IsIdempotent() {
        UUID o1 = UUID.randomUUID();

        ReservationResult first = reserveOrder("ghost", o1);
        ReservationResult second = reserveOrder("ghost", o1);

        assertThat(first.reserved()).isFalse();
        assertThat(second.reserved()).isFalse();
        assertThat(redis.keys("*")).containsExactly("inventory:order:" + o1);
    }

    @Test
    void seedDoesNotOverwriteAnExistingKey() {
        putEvent("e1", 1, 999, "USD");

        repository.seedIfAbsent("e1", 100, 12550, "EUR");

        assertThat(field("e1", "stock")).isEqualTo("1");
        assertThat(field("e1", "priceCents")).isEqualTo("999");
        assertThat(field("e1", "currency")).isEqualTo("USD");
    }

    @Test
    void seedWritesTheThreeFieldsWhenKeyIsAbsent() {
        repository.seedIfAbsent("e1", 100, 12550, "EUR");

        assertThat(field("e1", "stock")).isEqualTo("100");
        assertThat(field("e1", "priceCents")).isEqualTo("12550");
        assertThat(field("e1", "currency")).isEqualTo("EUR");
    }

    @Test
    void concurrentReservationsNeverOversell() throws Exception {
        int threads = 50;
        int stock = 20;
        putEvent("e1", stock, 12550, "EUR");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Callable<Boolean> task = () -> {
                start.await();
                return reserveNewOrder("e1", 1).reserved();
            };
            futures.add(pool.submit(task));
        }
        start.countDown();
        int successes = 0;
        for (Future<Boolean> f : futures) {
            if (f.get()) {
                successes++;
            }
        }
        pool.shutdown();

        assertThat(successes).isEqualTo(stock);
        assertThat(field("e1", "stock")).isEqualTo("0");
    }
}
