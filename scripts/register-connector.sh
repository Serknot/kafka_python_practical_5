set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONFIG_FILE="${CONFIG_FILE:-/connector-config/postgres-connector.json}"

echo "Ожидаю готовности Kafka Connect REST API (${CONNECT_URL})..."
until curl -s -f -o /dev/null "${CONNECT_URL}/connectors"; do
  echo "  ...ещё не готов, повтор через 5 секунд"
  sleep 5
done
echo "Kafka Connect REST API готов."

echo "Регистрирую коннектор из ${CONFIG_FILE}..."
HTTP_CODE=$(curl -s -o /tmp/register-response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors")

cat /tmp/register-response.json
echo

if [ "${HTTP_CODE}" = "201" ]; then
  echo "Коннектор успешно создан (HTTP 201)."
elif [ "${HTTP_CODE}" = "409" ]; then
  echo "Коннектор с таким именем уже существует (HTTP 409) — это нормально при повторном запуске."
else
  echo "Неожиданный код ответа: ${HTTP_CODE}"
  exit 1
fi

echo
echo "Статус коннектора:"
curl -s "${CONNECT_URL}/connectors/postgres-connector/status"
echo
