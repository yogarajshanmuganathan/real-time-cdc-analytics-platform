#!/bin/bash
# Deployment script for Debezium connectors

CONNECT_URL="http://kafka-connect:8083"
CONFIG_DIR="./config"

echo "Waiting for Kafka Connect to be ready..."
sleep 30

# Deploy PostgreSQL CDC Connector
echo "Deploying PostgreSQL CDC Connector..."
curl -X POST \
  -H "Content-Type: application/json" \
  -d @${CONFIG_DIR}/debezium-source.json \
  ${CONNECT_URL}/connectors

echo ""
echo "Connector deployed successfully!"

# Check connector status
echo ""
echo "Checking connector status..."
curl -s ${CONNECT_URL}/connectors/postgres-cdc-source/status | jq '.'

# List all topics created
echo ""
echo "Topics created:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
