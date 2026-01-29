#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# 1. Setup & Safety
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../../.."
ENV_FILE="${PROJECT_ROOT}/infrastructure/.env"
CONTAINER="infrastructure-clickhouse-1"

# 2. Load Environment Variables
if [ -f "$ENV_FILE" ]; then
    echo -e "${BLUE}Loading config from:${NC} $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo -e "${RED}Error: .env file not found at $ENV_FILE${NC}"
    exit 1
fi

# 3. Check Container Status
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo -e "${RED}Error: Container '$CONTAINER' is not running.${NC}"
    exit 1
fi

# 4. Helper Function
print_header() {
    echo -e "\n${BOLD}${CYAN}=== $1 ===${NC}"
}

run_query() {
    local text="$1"
    local query="$2"
    
    echo -e "${YELLOW}➤ $text${NC}"
    echo -e "${BLUE}  Query:${NC} $query"
    echo "---------------------------------------------------"
    
    # Run query and capture exit code
    if docker exec -i "$CONTAINER" clickhouse-client \
        -u "${CLICKHOUSE_USER}" \
        --password "${CLICKHOUSE_PASSWORD}" \
        --query "$query"; then
        : # Success
    else
        echo -e "${RED}Query Failed${NC}"
    fi
    echo ""
}

# 5. Execute Checks
echo -e "\n${GREEN}${BOLD}Starting ClickHouse Data Check${NC}"
echo -e "   User: ${BOLD}${CLICKHOUSE_USER}${NC}"
echo "---------------------------------------------------"

# Check 1
print_header "1. Database List"
run_query "Checking available databases" "SHOW DATABASES"

# Check 2
print_header "2. Tables in Finance DW"
run_query "Listing tables in ${CLICKHOUSE_DB:-finance_dw}" "SHOW TABLES FROM ${CLICKHOUSE_DB:-finance_dw}"

# Check 3
print_header "3. Data Volume"
run_query "Counting rows in paysim_txn" "SELECT count() as total_rows FROM ${CLICKHOUSE_DB:-finance_dw}.paysim_txn"

# Check 4
print_header "4. Data Sample"
run_query "Fetching 1 sample row" "SELECT * FROM ${CLICKHOUSE_DB:-finance_dw}.paysim_txn LIMIT 20 FORMAT Vertical"

echo -e "${GREEN}Check Completed Successfully.${NC}\n"
