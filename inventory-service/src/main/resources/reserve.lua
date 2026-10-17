-- KEYS[1] = inventory:event:{eventId}, ARGV[1] = quantite.
-- Tout ou rien, atomique (Redis execute un script d'un bloc). Cle absente = refus, aucune cle creee.
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {0}
end
local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))
local quantity = tonumber(ARGV[1])
if stock == nil or quantity == nil or quantity <= 0 or stock < quantity then
  return {0}
end
redis.call('HINCRBY', KEYS[1], 'stock', -quantity)
return {1, redis.call('HGET', KEYS[1], 'priceCents'), redis.call('HGET', KEYS[1], 'currency')}
