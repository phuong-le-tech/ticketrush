-- KEYS[1] = inventory:event:{eventId}, KEYS[2] = inventory:order:{orderId}
-- ARGV[1] = quantity, ARGV[2] = reservedUntil (epoch millis), ARGV[3] = dedupe TTL (seconds).
-- Atomic (Redis runs a script as one block): the dedupe check, the stock check, the decrement and
-- the stored outcome can never interleave with another order.
-- Returns {0} (rejected) or {1, priceCents, quantity, currency, reservedUntil}.

-- Already processed (replay or duplicate POST): return the first outcome, never touch stock again.
local seen = redis.call('HMGET', KEYS[2], 'reserved', 'priceCents', 'quantity', 'currency', 'reservedUntil')
if seen[1] == '1' then
  return {1, seen[2], seen[3], seen[4], seen[5]}
elseif seen[1] == '0' then
  return {0}
end

-- Unknown event (no stock field) is rejected like a sold-out one. The rejection is stored too, so this
-- orderId stays rejected even if the event is seeded later (a rejected order is never charged).
-- No key is created under KEYS[1] for an event that does not exist.
local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))
local quantity = tonumber(ARGV[1])
if stock == nil or quantity == nil or quantity <= 0 or stock < quantity then
  redis.call('HSET', KEYS[2], 'reserved', '0')
  redis.call('EXPIRE', KEYS[2], ARGV[3])
  return {0}
end

redis.call('HINCRBY', KEYS[1], 'stock', -quantity)
local priceCents = redis.call('HGET', KEYS[1], 'priceCents')
local currency = redis.call('HGET', KEYS[1], 'currency')
redis.call('HSET', KEYS[2], 'reserved', '1', 'priceCents', priceCents, 'quantity', ARGV[1],
  'currency', currency, 'reservedUntil', ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return {1, priceCents, ARGV[1], currency, ARGV[2]}
