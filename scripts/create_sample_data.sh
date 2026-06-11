#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAMPLE_DIR="$PROJECT_ROOT/data/sample"
SAMPLE_FILE="$SAMPLE_DIR/raw_events_sample.csv"

mkdir -p "$SAMPLE_DIR"

cat > "$SAMPLE_FILE" <<'EOF'
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
2019-10-01 00:00:00 UTC,view,3900821,2053013552326770905,appliances.environment.water_heater,aqua,33.20,554748717,9333dfbd-b87a-4708-9857-6336556b0fcc
2019-10-01 00:03:00 UTC,cart,3900821,2053013552326770905,appliances.environment.water_heater,aqua,33.20,554748717,9333dfbd-b87a-4708-9857-6336556b0fcc
2019-10-01 00:09:00 UTC,view,1307067,2053013558920217191,computers.notebook,lenovo,251.74,554748717,11111111-1111-1111-1111-111111111111
2019-10-01 15:00:00 UTC,view,1004856,2053013555631882655,electronics.smartphone,samsung,130.76,512742880,22222222-2222-2222-2222-222222222222
2019-10-01 15:04:59 UTC,purchase,1004856,2053013555631882655,electronics.smartphone,samsung,130.76,512742880,22222222-2222-2222-2222-222222222222
2019-10-01 15:09:59 UTC,view,1004856,2053013555631882655,electronics.smartphone,samsung,130.76,512742880,22222222-2222-2222-2222-222222222222
2019-10-31 18:00:00 UTC,view,1005000,2053013555631882655,electronics.smartphone,apple,999.99,999999999,33333333-3333-3333-3333-333333333333
EOF

echo "[sample] created: $SAMPLE_FILE"
echo "[sample] preview:"
head -n 3 "$SAMPLE_FILE"