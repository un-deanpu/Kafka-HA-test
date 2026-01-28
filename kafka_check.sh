#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPIC="${2:-healthcheck-topic}"

echo "== Kafka Health Check =="
echo "Bootstrap: $BOOTSTRAP_SERVER"
echo "Topic:     $TOPIC"
echo

# 1) 檢查 broker 是否可連線 + topic 是否可列出
echo "[1/4] Checking broker connectivity..."
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list > /dev/null
echo "OK: broker reachable"
echo

# 2) 確保 topic 存在 (不存在就建立)
echo "[2/4] Ensuring topic exists..."
if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$TOPIC" > /dev/null 2>&1; then
  echo "OK: topic exists"
else
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --topic "$TOPIC" --partitions 1 \
    --replication-factor 1 > /dev/null
  echo "OK: topic created"
fi
echo

# 3) Produce 一筆測試訊息 (帶唯一 ID)
MSG="healthcheck-$(date +%s)-$RANDOM"
echo "[3/4] Producing message: $MSG"
echo "$MSG" | kafka-console-producer --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TOPIC" > /dev/null
echo "OK: produced"
echo

# 4) Consume 讀回來 (只讀 1 筆, 避免卡住)
echo "[4/4] Consuming message..."
RECV=$(kafka-console-consumer \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms 5000 \
  --max-messages 10 2>/dev/null | grep "$MSG" || true)

if [[ "$RECV" == "$MSG" ]]; then
  echo
  echo "✅ SUCCESS: Kafka is working (produce/consume verified)"
  exit 0
else
  echo
  echo "❌ FAIL: did not receive produced message"
  exit 1
fi
