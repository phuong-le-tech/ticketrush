-- KEYS[1] = inventory:event:{eventId}, KEYS[2] = inventory:order:{orderId}
-- Gives back the tickets an order reserved, at most once. Atomic like reserve.lua: the check, the
-- restock and the "released" mark can never interleave with another release or reservation.
-- The quantity comes from the stored reservation, never from the message.
-- Returns the restocked quantity, 0 when there is nothing to give back.

local seen = redis.call('HMGET', KEYS[2], 'reserved', 'quantity', 'released')
-- Unknown order, rejected order, or already released: no-op, and no key is created.
if seen[1] ~= '1' or seen[3] == '1' then
  return 0
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end

redis.call('HINCRBY', KEYS[1], 'stock', seen[2])
redis.call('HSET', KEYS[2], 'released', '1')
return tonumber(seen[2])
