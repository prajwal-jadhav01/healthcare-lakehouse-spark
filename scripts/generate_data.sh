#!/usr/bin/env bash
# Generates synthetic patient data with Synthea (https://github.com/synthetichealth/synthea)
# into data/landing/batch_001, then derives batch_002 (changes + bad rows) from it.
#
#   ./scripts/generate_data.sh            # 1000 patients
#   PATIENTS=5000 ./scripts/generate_data.sh
#
# Needs Docker (runs Synthea in a Java container) and python3.
set -euo pipefail

PATIENTS="${PATIENTS:-1000}"
SEED="${SEED:-42}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="$ROOT/data/.synthea"
JAR="$CACHE/synthea-with-dependencies.jar"
JAR_URL="https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar"

mkdir -p "$CACHE" "$ROOT/data/landing"
if [ ! -f "$JAR" ]; then
  echo "Downloading Synthea (~200 MB, once)..."
  curl -fL -o "$JAR" "$JAR_URL"
fi

echo "Generating $PATIENTS patients (seed $SEED)..."
rm -rf "$CACHE/out"
docker run --rm -u "$(id -u):$(id -g)" -v "$CACHE:/work" -w /work eclipse-temurin:17-jre \
  java -jar synthea-with-dependencies.jar -p "$PATIENTS" -s "$SEED" \
    --exporter.baseDirectory ./out \
    --exporter.csv.export true \
    --exporter.csv.included_files patients.csv,encounters.csv,conditions.csv,medications.csv \
    --exporter.fhir.export false \
    --exporter.hospital.fhir.export false \
    --exporter.practitioner.fhir.export false \
  | tail -n 3

rm -rf "$ROOT/data/landing/batch_001" "$ROOT/data/landing/batch_002"
mkdir -p "$ROOT/data/landing/batch_001"
cp "$CACHE"/out/csv/{patients,encounters,conditions,medications}.csv "$ROOT/data/landing/batch_001/"
python3 "$ROOT/scripts/make_update_batch.py" "$ROOT/data/landing/batch_001" "$ROOT/data/landing/batch_002"

wc -l "$ROOT"/data/landing/batch_001/*.csv
