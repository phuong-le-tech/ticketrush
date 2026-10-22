-- KEYS[1] = inventory:event:{eventId}; ARGV = stock, priceCents, currency.
-- N'ecrit que si la cle est absente: un redemarrage ne remet pas le stock a zero.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], 'stock', ARGV[1], 'priceCents', ARGV[2], 'currency', ARGV[3])
return 1
