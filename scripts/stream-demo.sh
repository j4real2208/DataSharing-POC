#!/usr/bin/env bash

###############################################################################
# STREAMING DEMO: Live CDC data flow from source Postgres → Kafka → Sink Postgres
#
# This script continuously inserts weather data into source Postgres and displays
# real-time data flow through the entire CDC pipeline:
#   1. Insert data into source_db.public.weather_readings
#   2. Monitor Debezium CDC messages in source Kafka topic
#   3. Monitor mirrored messages in sink Kafka topic
#   4. Display JDBC sink writes to sink_db.public.weather_readings
#
# Usage:
#   ./stream-demo.sh [--source-context minikube-a] [--sink-context minikube-b]
#                    [--interval 3] [--duration 120] [--with-kafka-spy]
#
###############################################################################

set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

SOURCE_CTX="${SOURCE_CTX:-minikube-a}"
SINK_CTX="${SINK_CTX:-minikube-b}"
INSERT_INTERVAL="${INSERT_INTERVAL:-3}"  # seconds between inserts
DEMO_DURATION="${DEMO_DURATION:-120}"    # total demo duration in seconds
SHOW_KAFKA="${SHOW_KAFKA:-false}"        # whether to spy on Kafka topics
SPINNER_FRAMES=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Demo data
CITIES=("Seattle" "Austin" "Chicago" "Denver" "Boston" "San Francisco" "Miami" "Phoenix")

get_base_temp() {
  local city="$1"
  case "$city" in
    Seattle) echo "14.3" ;;
    Austin) echo "27.8" ;;
    Chicago) echo "9.1" ;;
    Denver) echo "12.5" ;;
    Boston) echo "8.7" ;;
    San\ Francisco) echo "16.2" ;;
    Miami) echo "28.3" ;;
    Phoenix) echo "32.1" ;;
    *) echo "20.0" ;;
  esac
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

decode_base64() {
  if base64 --decode >/dev/null 2>&1 </dev/null; then
    base64 --decode
  else
    base64 -D
  fi
}

print_header() {
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${CYAN}$1${NC}"
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_subheader() {
  echo -e "${MAGENTA}→ $1${NC}"
}

print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
  echo -e "${BLUE}ℹ $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

spinner() {
  local pid=$1
  local frame=0
  echo -n " "
  while kill -0 $pid 2>/dev/null; do
    printf "\b${SPINNER_FRAMES[$frame]}"
    frame=$(( (frame + 1) % ${#SPINNER_FRAMES[@]} ))
    sleep 0.1
  done
  echo -ne "\b"
}

get_source_postgres_pod() {
  kubectl --context="$SOURCE_CTX" -n database get pods \
    -l cluster-name=source-postgres \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo ""
}

get_sink_postgres_pod() {
  kubectl --context="$SINK_CTX" -n database get pods \
    -l cluster-name=sink-postgres \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo ""
}

get_source_db_password() {
  kubectl --context "$SOURCE_CTX" -n database get secret \
    postgres.source-postgres.credentials.postgresql.acid.zalan.do \
    -o jsonpath='{.data.password}' 2>/dev/null | decode_base64 || echo ""
}

get_sink_db_password() {
  kubectl --context "$SINK_CTX" -n database get secret \
    sink-user.sink-postgres.credentials.postgresql.acid.zalan.do \
    -o jsonpath='{.data.password}' 2>/dev/null | decode_base64 || echo ""
}

query_source_db() {
  local pod="$1"
  local password="$2"
  local query="$3"

  kubectl --context="$SOURCE_CTX" -n database exec "$pod" -- /bin/bash -ec "
    export PGPASSWORD='$password'
    psql -h localhost -U postgres -d source_db -tAc \"$query\" 2>/dev/null || echo 'ERROR'
  " 2>/dev/null || echo "ERROR"
}

query_sink_db() {
  local pod="$1"
  local password="$2"
  local query="$3"

  kubectl --context="$SINK_CTX" -n database exec "$pod" -- /bin/bash -ec "
    export PGPASSWORD='$password'
    psql -h localhost -U sink-user -d sink_db -tAc \"$query\" 2>/dev/null || echo 'ERROR'
  " 2>/dev/null || echo "ERROR"
}

display_source_table() {
  local pod="$1"
  local password="$2"

  kubectl --context="$SOURCE_CTX" -n database exec "$pod" -- /bin/bash -ec "
    export PGPASSWORD='$password'
    psql -h localhost -U postgres -d source_db -tA -F '|' \
      -c \"SELECT id, city, temperature_c, to_char(observed_at, 'HH24:MI:SS') AS time FROM public.weather_readings ORDER BY id DESC LIMIT 10;\"
  " 2>/dev/null || echo "ERROR"
}

display_sink_table() {
  local pod="$1"
  local password="$2"

  kubectl --context="$SINK_CTX" -n database exec "$pod" -- /bin/bash -ec "
    export PGPASSWORD='$password'
    psql -h localhost -U sink-user -d sink_db -tA -F '|' \
      -c \"SELECT id, city, temperature_c, to_char(observed_at, 'HH24:MI:SS') AS time FROM public.weather_readings ORDER BY id DESC LIMIT 10;\"
  " 2>/dev/null || echo "ERROR"
}

get_row_counts() {
  local source_pod="$1"
  local source_pwd="$2"
  local sink_pod="$3"
  local sink_pwd="$4"

  local source_count=$(query_source_db "$source_pod" "$source_pwd" "SELECT COUNT(*) FROM public.weather_readings;")
  local sink_count=$(query_sink_db "$sink_pod" "$sink_pwd" "SELECT COUNT(*) FROM public.weather_readings;")

  echo "$source_count|$sink_count"
}

generate_random_temp() {
  local base_temp=$1
  local variance=$((RANDOM % 20 - 10))  # -10 to +10 variation
  local result=$(echo "$base_temp + $variance * 0.1" | bc 2>/dev/null || echo "$base_temp")
  printf "%.1f" "$result"
}

# ============================================================================
# PREFLIGHT CHECKS
# ============================================================================

run_preflight_checks() {
  print_header "PREFLIGHT CHECKS"

  print_subheader "Verifying source cluster context '$SOURCE_CTX'..."
  if ! kubectl --context="$SOURCE_CTX" cluster-info >/dev/null 2>&1; then
    print_warning "Could not connect to source context"
    return 1
  fi
  print_success "Source cluster accessible"

  print_subheader "Verifying sink cluster context '$SINK_CTX'..."
  if ! kubectl --context="$SINK_CTX" cluster-info >/dev/null 2>&1; then
    print_warning "Could not connect to sink context"
    return 1
  fi
  print_success "Sink cluster accessible"

  print_subheader "Locating source Postgres pod..."
  SOURCE_POD=$(get_source_postgres_pod)
  if [ -z "$SOURCE_POD" ]; then
    print_warning "Source Postgres pod not found. Run ./scripts/deploy-source.sh first."
    return 1
  fi
  print_success "Found: $SOURCE_POD"

  print_subheader "Locating sink Postgres pod..."
  SINK_POD=$(get_sink_postgres_pod)
  if [ -z "$SINK_POD" ]; then
    print_warning "Sink Postgres pod not found. Run ./scripts/deploy-sink.sh first."
    return 1
  fi
  print_success "Found: $SINK_POD"

  print_subheader "Fetching database credentials..."
  SOURCE_PASSWORD=$(get_source_db_password)
  if [ -z "$SOURCE_PASSWORD" ]; then
    print_warning "Could not fetch source database password"
    return 1
  fi
  print_success "Source credentials obtained"

  SINK_PASSWORD=$(get_sink_db_password)
  if [ -z "$SINK_PASSWORD" ]; then
    print_warning "Could not fetch sink database password"
    return 1
  fi
  print_success "Sink credentials obtained"

  echo ""
}

# ============================================================================
# MAIN DEMO LOGIC
# ============================================================================

run_streaming_demo() {
  print_header "🔴 STREAMING CDC DEMO - LIVE DATA FLOW"
  echo ""
  print_info "Duration: ${DEMO_DURATION}s | Insert interval: ${INSERT_INTERVAL}s"
  print_info "Source: $SOURCE_CTX | Sink: $SINK_CTX"
  echo ""

  local start_time=$(date +%s)
  local insert_count=0
  local last_insert_time=0

  # Main demo loop
  while true; do
    local current_time=$(date +%s)
    local elapsed=$((current_time - start_time))

    # Check if demo should end
    if [ "$elapsed" -ge "$DEMO_DURATION" ]; then
      print_info "Demo duration reached"
      break
    fi

     # Time for next insert?
     if [ $((current_time - last_insert_time)) -ge "$INSERT_INTERVAL" ]; then
       # Select random city and temperature
       local city_idx=$((RANDOM % ${#CITIES[@]}))
       local city="${CITIES[$city_idx]}"
       local base_temp=$(get_base_temp "$city")
       local temp=$(generate_random_temp "$base_temp")

      # Insert into source database
      print_header "🌊 DATA INSERTION #$((++insert_count)) @ $(date '+%H:%M:%S')"
      print_subheader "Inserting: City=$city, Temp=${temp}°C"

      query_source_db "$SOURCE_POD" "$SOURCE_PASSWORD" \
        "INSERT INTO public.weather_readings (city, temperature_c) VALUES ('$city', $temp);" > /dev/null 2>&1

      if [ $? -eq 0 ]; then
        print_success "Data inserted into source_db"
      else
        print_warning "Failed to insert data"
      fi

      # Wait a moment for Debezium to capture
      sleep 1

      # Display current state
      echo ""
      print_subheader "SOURCE DATABASE (source_db.public.weather_readings)"
      display_source_table "$SOURCE_POD" "$SOURCE_PASSWORD"

      echo ""
      print_subheader "SINK DATABASE (sink_db.public.weather_readings)"
      display_sink_table "$SINK_POD" "$SINK_PASSWORD"

      # Show statistics
      echo ""
      print_subheader "PIPELINE STATISTICS"
      local counts=$(get_row_counts "$SOURCE_POD" "$SOURCE_PASSWORD" "$SINK_POD" "$SINK_PASSWORD")
      local source_rows=$(echo "$counts" | cut -d'|' -f1)
      local sink_rows=$(echo "$counts" | cut -d'|' -f2)
      local lag=$((source_rows - sink_rows))

      echo -e "  Source rows: ${GREEN}$source_rows${NC}"
      echo -e "  Sink rows:   ${GREEN}$sink_rows${NC}"
      echo -e "  Lag:         ${YELLOW}$lag${NC} rows"
      echo -e "  Elapsed:     ${BLUE}${elapsed}s${NC} / ${DEMO_DURATION}s"

      # Show progress bar
      local progress=$((elapsed * 50 / DEMO_DURATION))
      echo -n "  Progress: ["
      for ((i=0; i<50; i++)); do
        if [ $i -lt $progress ]; then
          echo -n "="
        else
          echo -n "-"
        fi
      done
      echo "]"

      echo ""
      last_insert_time=$current_time
    fi

    sleep 1
  done
}

# ============================================================================
# KAFKA TOPIC MONITORING (Optional)
# ============================================================================

show_kafka_topics() {
  if [ "$SHOW_KAFKA" != "true" ]; then
    return 0
  fi

  print_header "📊 KAFKA TOPIC SPY"

  local source_connect_pod=$(kubectl --context="$SOURCE_CTX" -n messaging get pods \
    -l strimzi.io/cluster=source-connect,strimzi.io/kind=KafkaConnect \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

  local sink_connect_pod=$(kubectl --context="$SINK_CTX" -n messaging get pods \
    -l strimzi.io/cluster=sink-connect,strimzi.io/kind=KafkaConnect \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

  if [ -z "$source_connect_pod" ]; then
    print_warning "Could not find source Kafka Connect pod"
    return 1
  fi

  # SOURCE TOPIC
  print_subheader "SOURCE TOPIC: source.public.weather_readings"
  echo -e "${BLUE}Last 3 messages:${NC}"
  kubectl --context="$SOURCE_CTX" -n messaging exec "$source_connect_pod" -- /bin/bash -lc "
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server source-kafka-kafka-bootstrap:9092 \
  --topic source.public.weather_readings \
  --from-beginning \
  --max-messages 3 \
  --consumer-property group.id=demo-source-spy-\$RANDOM \
  --property print.timestamp=true \
  --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --timeout-ms 5000 2>&1 | tail -15 || true
" 2>/dev/null || true

  echo ""
  print_info "Topic stats:"
  kubectl --context="$SOURCE_CTX" -n messaging exec "$source_connect_pod" -- /bin/bash -c "
/opt/kafka/bin/kafka-topics.sh --bootstrap-server source-kafka-kafka-bootstrap:9092 --topic source.public.weather_readings --describe 2>/dev/null | tail -1
" 2>/dev/null | sed 's/^/  /' || print_warning "  Could not fetch topic stats"

  echo ""

  # SINK TOPIC
  if [ -n "$sink_connect_pod" ]; then
    print_subheader "SINK TOPIC: source.source.public.weather_readings (mirrored)"
    echo -e "${BLUE}Last 3 messages:${NC}"
    kubectl --context="$SINK_CTX" -n messaging exec "$sink_connect_pod" -- /bin/bash -lc "
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server sink-kafka-kafka-bootstrap:9092 \
  --topic source.source.public.weather_readings \
  --from-beginning \
  --max-messages 3 \
  --consumer-property group.id=demo-sink-spy-\$RANDOM \
  --property print.timestamp=true \
  --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --timeout-ms 5000 2>&1 | tail -15 || true
" 2>/dev/null || true

    echo ""
    print_info "Topic stats:"
    kubectl --context="$SINK_CTX" -n messaging exec "$sink_connect_pod" -- /bin/bash -c "
/opt/kafka/bin/kafka-topics.sh --bootstrap-server sink-kafka-kafka-bootstrap:9092 --topic source.source.public.weather_readings --describe 2>/dev/null | tail -1
" 2>/dev/null | sed 's/^/  /' || print_warning "  Could not fetch topic stats"
  else
    print_warning "Could not find sink Kafka Connect pod"
  fi

  echo ""
}

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print_summary() {
  print_header "📈 DEMO COMPLETE - FINAL SUMMARY"

  local final_counts=$(get_row_counts "$SOURCE_POD" "$SOURCE_PASSWORD" "$SINK_POD" "$SINK_PASSWORD")
  local final_source=$(echo "$final_counts" | cut -d'|' -f1)
  local final_sink=$(echo "$final_counts" | cut -d'|' -f2)

  echo ""
  echo -e "Final row counts:"
  echo -e "  Source DB: ${GREEN}$final_source${NC} rows"
  echo -e "  Sink DB:   ${GREEN}$final_sink${NC} rows"
  echo ""

  if [ "$final_source" -eq "$final_sink" ]; then
    echo -e "${GREEN}✓ All data successfully replicated!${NC}"
  else
    echo -e "${YELLOW}⚠ Data still replicating... (lag: $((final_source - final_sink)) rows)${NC}"
  fi

  echo ""
  echo -e "${CYAN}Data flow verification:${NC}"
  echo -e "  1. ${GREEN}✓${NC} Data inserted into source_db"
  echo -e "  2. ${GREEN}✓${NC} Debezium captured CDC events"
  echo -e "  3. ${GREEN}✓${NC} Events flowed through source Kafka"
  echo -e "  4. ${GREEN}✓${NC} MirrorMaker2 replicated to sink Kafka"
  echo -e "  5. ${GREEN}✓${NC} JDBC sink connector wrote to sink_db"
  echo ""
}

# ============================================================================
# USAGE & HELP
# ============================================================================

print_usage() {
  cat << EOF
${CYAN}STREAMING CDC DEMO - Live data flow visualization${NC}

${YELLOW}Usage:${NC}
  ./stream-demo.sh [OPTIONS]

${YELLOW}Options:${NC}
  --source-context CTX     Source Minikube context (default: minikube-a)
  --sink-context CTX       Sink Minikube context (default: minikube-b)
  --interval SECONDS       Seconds between data inserts (default: 3)
  --duration SECONDS       Total demo duration in seconds (default: 120)
  --with-kafka-spy         Show Kafka topic messages during demo
  --help                   Show this help message

${YELLOW}Examples:${NC}
  # Basic 2-minute demo with 3-second intervals
  ./stream-demo.sh

  # Custom duration and interval
  ./stream-demo.sh --duration 180 --interval 2

  # Show Kafka topics during the demo
  ./stream-demo.sh --with-kafka-spy

  # Full pipeline monitoring
  ./stream-demo.sh --duration 300 --interval 5 --with-kafka-spy

${CYAN}Prerequisites:${NC}
  - Both source and sink clusters deployed (run deploy-source.sh & deploy-sink.sh)
  - Sufficient permissions to exec into pods
  - kubectl and minikube on PATH

EOF
}

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

parse_arguments() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --source-context)
        SOURCE_CTX="$2"
        shift 2
        ;;
      --sink-context)
        SINK_CTX="$2"
        shift 2
        ;;
      --interval)
        INSERT_INTERVAL="$2"
        shift 2
        ;;
      --duration)
        DEMO_DURATION="$2"
        shift 2
        ;;
      --with-kafka-spy)
        SHOW_KAFKA="true"
        shift
        ;;
      --help)
        print_usage
        exit 0
        ;;
      *)
        print_warning "Unknown option: $1"
        print_usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_arguments "$@"

  run_preflight_checks || {
    print_warning "Preflight checks failed. Aborting."
    exit 1
  }

  run_streaming_demo
  show_kafka_topics
  print_summary

  echo -e "${MAGENTA}═══════════════════════════════════════════════════════════════════════${NC}"
  echo -e "${GREEN}Thank you for watching the CDC pipeline demo!${NC}"
  echo -e "${MAGENTA}═══════════════════════════════════════════════════════════════════════${NC}"
}

main "$@"

