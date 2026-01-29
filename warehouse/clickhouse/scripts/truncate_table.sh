#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Setup
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../../.."
source "${PROJECT_ROOT}/infrastructure/.env"
CONTAINER="infrastructure-clickhouse-1"

echo -e "${YELLOW}WARNING: This will delete ALL data from finance_dw.paysim_txn${NC}"
read -p "Are you sure? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo -e "\n${YELLOW}➤ Truncating table finance_dw.paysim_txn...${NC}"

if docker exec -i "$CONTAINER" clickhouse-client \
    -u "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    --query "TRUNCATE TABLE finance_dw.paysim_txn"; then
    echo -e "${GREEN}Table truncated successfully.${NC}"
else
    echo -e "${RED}Failed to truncate table.${NC}"
fi
