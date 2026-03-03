#!/bin/bash
###############################################################################
# kafka-topics-config-modify.sh
# Configure Kafka topic retention, min.insync.replicas, and replication factor
#
# Usage:
#   ./kafka-topics-config-modify.sh --bootstrap-server <server> --topic-prefix <prefix> --retention-days <days> [--min-insync-replicas <n>] [--replication-factor <n>] [--dry-run]
#   ./kafka-topics-config-modify.sh --bootstrap-server <server> --topics-file <file>   --retention-days <days> [--min-insync-replicas <n>] [--replication-factor <n>] [--dry-run]
#
# Examples:
#   # Using prefix — set 1 day retention + min ISR 2 on all topics starting with "accdrmig"
#   ./kafka-topics-config-modify.sh --bootstrap-server wn1-datami.xxx.internal.cloudapp.net:9092 --topic-prefix accdrmig --retention-days 1 --min-insync-replicas 2 --replication-factor 3 --dry-run
#
#   # Using file — also change replication factor to 3
#   ./kafka-topics-config-modify.sh --bootstrap-server wn1-datami.xxx.internal.cloudapp.net:9092 --topics-file topics.txt --retention-days 1 --min-insync-replicas 2 --replication-factor 3
#
#   # Dry run (show what would be done without applying)
#   ./kafka-topics-config-modify.sh --bootstrap-server wn1-datami.xxx.internal.cloudapp.net:9092 --topic-prefix accdrmig --retention-days 1 --min-insync-replicas 2 --dry-run
###############################################################################

set -euo pipefail

# ---- Defaults ----
BOOTSTRAP_SERVER=""
TOPIC_PREFIX=""
TOPICS_FILE=""
RETENTION_DAYS=""
MIN_INSYNC_REPLICAS=""
REPLICATION_FACTOR=""
DRY_RUN=false
KAFKA_BIN="/usr/hdp/current/kafka-broker/bin"

# ---- Colors ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo ""
    echo "Usage:"
    echo "  $0 --bootstrap-server <server> (--topic-prefix <prefix> | --topics-file <file>) --retention-days <days> [--min-insync-replicas <n>] [--replication-factor <n>] [--dry-run]"
    echo ""
    echo "Required:"
    echo "  --bootstrap-server       Kafka bootstrap server (e.g., wn1-datami.xxx.internal.cloudapp.net:9092)"
    echo "  --topic-prefix           Topic name prefix to match (e.g., accdrmig)  -- OR --"
    echo "  --topics-file            Path to text file with one topic name per line"
    echo "  --retention-days         Retention period in days (e.g., 1, 7, 30)"
    echo ""
    echo "Optional:"
    echo "  --min-insync-replicas    min.insync.replicas value (e.g., 2)"
    echo "  --replication-factor     Replication factor (e.g., 3) — triggers partition reassignment"
    echo "  --dry-run                Show what would be done without applying changes"
    echo "  --help                   Show this help message"
    echo ""
    exit 1
}

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ---- Parse Arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bootstrap-server)       BOOTSTRAP_SERVER="$2"; shift 2 ;;
        --topic-prefix)           TOPIC_PREFIX="$2"; shift 2 ;;
        --topics-file)            TOPICS_FILE="$2"; shift 2 ;;
        --retention-days)         RETENTION_DAYS="$2"; shift 2 ;;
        --min-insync-replicas)    MIN_INSYNC_REPLICAS="$2"; shift 2 ;;
        --replication-factor)     REPLICATION_FACTOR="$2"; shift 2 ;;
        --dry-run)                DRY_RUN=true; shift ;;
        --help)                   usage ;;
        *) log_error "Unknown option: $1"; usage ;;
    esac
done

# ---- Validate Required Args ----
if [[ -z "$BOOTSTRAP_SERVER" ]]; then
    log_error "--bootstrap-server is required."
    usage
fi

if [[ -z "$TOPIC_PREFIX" && -z "$TOPICS_FILE" ]]; then
    log_error "Either --topic-prefix or --topics-file is required."
    usage
fi

if [[ -n "$TOPIC_PREFIX" && -n "$TOPICS_FILE" ]]; then
    log_error "Specify only one: --topic-prefix OR --topics-file, not both."
    usage
fi

if [[ -z "$RETENTION_DAYS" ]]; then
    log_error "--retention-days is required."
    usage
fi

if [[ -n "$TOPICS_FILE" && ! -f "$TOPICS_FILE" ]]; then
    log_error "Topics file not found: $TOPICS_FILE"
    exit 1
fi

# ---- Convert retention days to milliseconds ----
RETENTION_MS=$((RETENTION_DAYS * 86400000))

# ---- Resolve topic list ----
declare -a TOPICS=()

if [[ -n "$TOPIC_PREFIX" ]]; then
    log_info "Discovering topics with prefix: ${TOPIC_PREFIX}"
    while IFS= read -r topic; do
        [[ -n "$topic" ]] && TOPICS+=("$topic")
    done < <(${KAFKA_BIN}/kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null | grep "^${TOPIC_PREFIX}")
else
    log_info "Reading topics from file: ${TOPICS_FILE}"
    while IFS= read -r topic; do
        topic=$(echo "$topic" | xargs)  # trim whitespace
        [[ -n "$topic" && ! "$topic" =~ ^# ]] && TOPICS+=("$topic")
    done < "$TOPICS_FILE"
fi

if [[ ${#TOPICS[@]} -eq 0 ]]; then
    log_error "No topics found. Check your prefix or file."
    exit 1
fi

# ---- Summary ----
echo ""
echo "============================================================"
echo "  Kafka Topic Configuration Modify"
echo "============================================================"
echo "  Bootstrap Server      : $BOOTSTRAP_SERVER"
echo "  Topics matched        : ${#TOPICS[@]}"
echo "  retention.ms          : ${RETENTION_MS} (${RETENTION_DAYS} day(s))"
[[ -n "$MIN_INSYNC_REPLICAS" ]] && echo "  min.insync.replicas   : $MIN_INSYNC_REPLICAS"
[[ -n "$REPLICATION_FACTOR" ]]  && echo "  replication.factor    : $REPLICATION_FACTOR (reassignment)"
$DRY_RUN && echo -e "  Mode                  : ${YELLOW}DRY RUN${NC}"
echo "============================================================"
echo ""
echo "Topics:"
for t in "${TOPICS[@]}"; do echo "  - $t"; done
echo ""

if ! $DRY_RUN; then
    read -p "Proceed with changes? (y/N): " confirm
    [[ "$confirm" != "y" && "$confirm" != "Y" ]] && { log_warn "Aborted."; exit 0; }
fi

# ---- Build config string ----
CONFIG_PARTS=()
CONFIG_PARTS+=("retention.ms=${RETENTION_MS}")
[[ -n "$MIN_INSYNC_REPLICAS" ]] && CONFIG_PARTS+=("min.insync.replicas=${MIN_INSYNC_REPLICAS}")
CONFIG_STRING=$(IFS=,; echo "${CONFIG_PARTS[*]}")

# ---- Apply retention + min ISR ----
log_info "Applying topic configs: ${CONFIG_STRING}"
echo ""

SUCCESS=0
FAIL=0

for topic in "${TOPICS[@]}"; do
    if $DRY_RUN; then
        echo -e "  ${YELLOW}[DRY RUN]${NC} Would alter $topic -> $CONFIG_STRING"
    else
        if ${KAFKA_BIN}/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
            --entity-type topics --entity-name "$topic" \
            --alter --add-config "$CONFIG_STRING" 2>&1; then
            echo -e "  ${GREEN}[OK]${NC} $topic"
            ((SUCCESS++))
        else
            echo -e "  ${RED}[FAIL]${NC} $topic"
            ((FAIL++))
        fi
    fi
done

echo ""
if ! $DRY_RUN; then
    log_info "Config results: ${SUCCESS} succeeded, ${FAIL} failed out of ${#TOPICS[@]} topics"
fi

# ---- Replication Factor Change (Partition Reassignment) ----
if [[ -n "$REPLICATION_FACTOR" ]]; then
    echo ""
    log_info "Preparing replication factor change to ${REPLICATION_FACTOR}..."

    # Get broker IDs from kafka-broker-api-versions output
    # Format: wn0-datami.xxx:9092 (id: 1004 rack: /rack-1) -> (
    ALL_BROKER_IDS=$(${KAFKA_BIN}/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null \
        | grep -oP '\(id:\s*\K[0-9]+' | sort -un)

    if [[ -z "$ALL_BROKER_IDS" ]]; then
        log_warn "Could not auto-detect broker IDs. Falling back to topic metadata..."
        ALL_BROKER_IDS=$(${KAFKA_BIN}/kafka-topics.sh --describe --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null \
            | grep -oP 'Replicas:\s*\K[0-9,]+' | tr ',' '\n' | sort -un)
    fi

    TOTAL_BROKERS=$(echo "$ALL_BROKER_IDS" | wc -l)
    log_info "Discovered ${TOTAL_BROKERS} brokers: $(echo $ALL_BROKER_IDS | tr '\n' ' ')"

    if [[ "$REPLICATION_FACTOR" -gt "$TOTAL_BROKERS" ]]; then
        log_error "Replication factor (${REPLICATION_FACTOR}) cannot exceed number of brokers (${TOTAL_BROKERS})."
        exit 1
    fi

    # Convert to array for round-robin assignment
    readarray -t BROKER_ID_ARRAY <<< "$ALL_BROKER_IDS"
    BROKER_ARRAY=$(echo "$ALL_BROKER_IDS" | tr '\n' ',' | sed 's/,$//')
    log_info "Using brokers: [${BROKER_ARRAY}]"

    REASSIGN_FILE="/tmp/kafka-reassign-$(date +%s).json"

    # Build reassignment JSON
    echo '{"version":1,"partitions":[' > "$REASSIGN_FILE"

    FIRST=true
    PARTITION_INDEX=0
    for topic in "${TOPICS[@]}"; do
        PART_COUNT=$(${KAFKA_BIN}/kafka-topics.sh --describe --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$topic" 2>/dev/null \
            | head -1 | grep -oP 'PartitionCount:\s*\K[0-9]+')

        if [[ -z "$PART_COUNT" ]]; then
            log_warn "Skipping $topic — could not determine partition count"
            continue
        fi

        for ((p=0; p<PART_COUNT; p++)); do
            # Round-robin: shift starting broker per partition for even distribution
            REPLICAS=""
            for ((r=0; r<REPLICATION_FACTOR; r++)); do
                idx=$(( (PARTITION_INDEX + r) % TOTAL_BROKERS ))
                [[ -n "$REPLICAS" ]] && REPLICAS+=","
                REPLICAS+="${BROKER_ID_ARRAY[$idx]}"
            done

            if $FIRST; then
                FIRST=false
            else
                echo "," >> "$REASSIGN_FILE"
            fi
            printf '{"topic":"%s","partition":%d,"replicas":[%s]}' "$topic" "$p" "$REPLICAS" >> "$REASSIGN_FILE"
            ((PARTITION_INDEX++))
        done
    done

    echo "" >> "$REASSIGN_FILE"
    echo "]}" >> "$REASSIGN_FILE"

    if $DRY_RUN; then
        log_warn "[DRY RUN] Reassignment JSON written to: $REASSIGN_FILE"
        echo "Preview:"
        cat "$REASSIGN_FILE" | python -m json.tool 2>/dev/null || cat "$REASSIGN_FILE"
    else
        log_warn "Replication factor change will move data across brokers. This uses disk I/O and network."
        read -p "Apply partition reassignment now? (y/N): " confirm_repl
        if [[ "$confirm_repl" == "y" || "$confirm_repl" == "Y" ]]; then
            ${KAFKA_BIN}/kafka-reassign-partitions.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
                --reassignment-json-file "$REASSIGN_FILE" --execute
            log_info "Reassignment started. Monitor progress with:"
            echo "  ${KAFKA_BIN}/kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP_SERVER --reassignment-json-file $REASSIGN_FILE --verify"
        else
            log_warn "Reassignment skipped. JSON saved at: $REASSIGN_FILE"
        fi
    fi
fi

echo ""
log_info "Done. Verify with:"
echo "  ${KAFKA_BIN}/kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER --entity-type topics --entity-name <TOPIC> --describe"
echo "  df -h /mnt/data0 /mnt/data1"
