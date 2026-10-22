package com.ticketrush.gateway;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Deterministic orderId: UUIDv5 (RFC 9562, SHA-1) of (customerId, Idempotency-Key). A client retry
 * with the same key therefore maps to the same order, without the gateway storing anything.
 */
final class OrderIds {

    private static final UUID NAMESPACE = UUID.fromString("3fc18c52-d4b9-4f65-81fe-4ce095a8f1cc");
    private static final int VERSION_5 = 0x50;
    private static final int VARIANT_RFC = 0x80;

    private OrderIds() {
    }

    /** The length prefix keeps the name unambiguous: ("a:b", "c") and ("a", "b:c") never collide. */
    static UUID fromIdempotencyKey(String customerId, String idempotencyKey) {
        String name = customerId.length() + ":" + customerId + ":" + idempotencyKey;
        MessageDigest sha1 = sha1();
        sha1.update(ByteBuffer.allocate(16)
                .putLong(NAMESPACE.getMostSignificantBits())
                .putLong(NAMESPACE.getLeastSignificantBits())
                .array());
        byte[] hash = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
        hash[6] = (byte) ((hash[6] & 0x0f) | VERSION_5);
        hash[8] = (byte) ((hash[8] & 0x3f) | VARIANT_RFC);
        ByteBuffer bits = ByteBuffer.wrap(hash, 0, 16);
        return new UUID(bits.getLong(), bits.getLong());
    }

    private static MessageDigest sha1() {
        // UUIDv5 (RFC 9562) requires SHA-1 by spec; NOT a security/auth context (non-sensitive deterministic id).
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // Every Java SE runtime must provide SHA-1, so this is a broken JVM, not a request error.
            throw new IllegalStateException("SHA-1 not available", e);
        }
    }
}
