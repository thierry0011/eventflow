#!/usr/bin/env bash
# End-to-end smoke test. Reads top to bottom like the story of one order.
#
# Run it, then run `make logs` in another terminal and watch the same order id
# travel through all three services. That is the single most useful thing you
# can do on day one.
set -euo pipefail

ORDERS=http://localhost:8001
INVENTORY=http://localhost:8002
NOTIFS=http://localhost:8003

say() { printf "\n\033[1;36m==> %s\033[0m\n" "$1"; }

say "1. Health checks"
curl -fsS $ORDERS/health/ready   | python3 -m json.tool
curl -fsS $INVENTORY/health/ready | python3 -m json.tool
curl -fsS $NOTIFS/health/ready    | python3 -m json.tool

say "2. Get a token (demo user seeded by 'make seed')"
TOKEN=$(curl -fsS -X POST $ORDERS/api/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo","password":"demo12345"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access"])')
echo "token acquired (${#TOKEN} chars)"

say "3. Stock level before"
curl -fsS $INVENTORY/api/v1/products/SKU-LAPTOP-01 | python3 -m json.tool

say "4. Place an order (note the Idempotency-Key — replaying it is a no-op)"
IDEM=$(python3 -c 'import uuid;print(uuid.uuid4())')
ORDER=$(curl -fsS -X POST $ORDERS/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM" \
  -d '{"items":[{"sku":"SKU-LAPTOP-01","quantity":2}]}')
echo "$ORDER" | python3 -m json.tool
ORDER_ID=$(echo "$ORDER" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

say "5. Replay the exact same request — same Idempotency-Key"
curl -fsS -X POST $ORDERS/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM" \
  -d '{"items":[{"sku":"SKU-LAPTOP-01","quantity":2}]}' | python3 -m json.tool
echo "^ same order id, stock deducted once. That is idempotency."

say "6. Wait for the saga to settle (order.created -> reserve -> confirm)"
for i in $(seq 1 20); do
  STATUS=$(curl -fsS $ORDERS/api/v1/orders/$ORDER_ID -H "Authorization: Bearer $TOKEN" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  t+${i}s status=$STATUS"
  [[ "$STATUS" == "PENDING" ]] || break
  sleep 1
done

say "7. Stock level after"
curl -fsS $INVENTORY/api/v1/products/SKU-LAPTOP-01 | python3 -m json.tool

say "8. Notifications produced for this order"
curl -fsS "$NOTIFS/api/v1/notifications?order_id=$ORDER_ID" | python3 -m json.tool

say "9. Now the failure path: order more than exists (valid input, but stock is only 25)"
curl -fsS -X POST $ORDERS/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(python3 -c 'import uuid;print(uuid.uuid4())')" \
  -d '{"items":[{"sku":"SKU-LAPTOP-01","quantity":500}]}' | python3 -m json.tool
echo "^ accepted as PENDING, then compensated to CANCELLED by the saga."
echo "  An event-driven system says 'I have taken your request', not 'done'."

say "Done. Read docs/01-ARCHITECTURE.md with these logs open beside you."
