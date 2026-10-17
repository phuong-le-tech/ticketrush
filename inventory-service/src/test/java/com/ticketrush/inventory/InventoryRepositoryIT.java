package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
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
        repository = new InventoryRepository(redis);
    }

    private static String key(String eventId) {
        return "inventory:event:" + eventId;
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

        ReservationResult result = repository.reserve("e1", 2);

        assertThat(result.reserved()).isTrue();
        assertThat(result.priceCents()).isEqualTo(12550L);
        assertThat(result.currency()).isEqualTo("EUR");
        assertThat(field("e1", "stock")).isEqualTo("1");
    }

    @Test
    void reserveIsAllOrNothingWhenStockIsInsufficient() {
        putEvent("e1", 3, 12550, "EUR");
        assertThat(repository.reserve("e1", 2).reserved()).isTrue();

        ReservationResult second = repository.reserve("e1", 2);

        assertThat(second.reserved()).isFalse();
        assertThat(field("e1", "stock")).isEqualTo("1");
    }

    @Test
    void reservingExactlyTheRemainingStockSucceedsThenStockNeverGoesNegative() {
        putEvent("e1", 2, 12550, "EUR");

        assertThat(repository.reserve("e1", 2).reserved()).isTrue();
        assertThat(field("e1", "stock")).isEqualTo("0");

        assertThat(repository.reserve("e1", 1).reserved()).isFalse();
        assertThat(field("e1", "stock")).isEqualTo("0");
    }

    @Test
    void unknownEventIsRejectedAndNoKeyIsCreated() {
        ReservationResult result = repository.reserve("ghost", 1);

        assertThat(result.reserved()).isFalse();
        assertThat(redis.keys("*")).isEmpty();
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
                return repository.reserve("e1", 1).reserved();
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
