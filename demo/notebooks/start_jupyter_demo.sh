#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VAULT_IMAGE="${VAULT_IMAGE:-hashicorp/vault:latest}"
JUPYTER_IMAGE="${JUPYTER_IMAGE:-quay.io/jupyter/pyspark-notebook:latest}"
VAULT_CONTAINER="${VAULT_CONTAINER:-kebab-storm-vault}"
JUPYTER_CONTAINER="${JUPYTER_CONTAINER:-kebab-storm-jupyter}"
DEMO_NETWORK="${DEMO_NETWORK:-kebab-storm-demo}"
VAULT_PORT="${VAULT_PORT:-8200}"
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
SPARK_UI_PORT="${SPARK_UI_PORT:-4040}"
VAULT_DEV_TOKEN="${VAULT_DEV_TOKEN:-kebab-storm-demo-token}"
JUPYTER_TOKEN="${JUPYTER_TOKEN:-kebab-storm-demo-token}"
VAULT_SECRET_PATH="${VAULT_SECRET_PATH:-secret/kebab-storm/personal-health-records}"
SCENARIO_PATH="${PROJECT_ROOT}/scenario/personal_health_records_scenario.json"
CONTAINER_PROJECT_PATH="/home/jovyan/work/kebab_storm"
ANALYTICS_PACKAGE_PATH="${PROJECT_ROOT}/packages/kebab_storm_analytics"
CONTAINER_ANALYTICS_PACKAGE_PATH="${CONTAINER_PROJECT_PATH}/packages/kebab_storm_analytics"
DEMO_LABEL="io.kebab-storm.demo"
DEMO_VERSION_LABEL="io.kebab-storm.demo.version"
DEMO_VERSION="2"

usage() {
  cat <<EOF
Usage: ./demo/notebooks/start_jupyter_demo.sh

Starts the local Kebab Storm Vault and Jupyter PySpark demonstration.

Optional environment overrides:
  VAULT_IMAGE, JUPYTER_IMAGE
  VAULT_PORT, JUPYTER_PORT, SPARK_UI_PORT
  VAULT_DEV_TOKEN, JUPYTER_TOKEN
  VAULT_CONTAINER, JUPYTER_CONTAINER, DEMO_NETWORK
  VAULT_SECRET_PATH

Vault runs in insecure, in-memory development mode. Use this script only for
the local demonstration; it is not a production notebook or Vault deployment.
It uses fixed development tokens, HTTP Vault, local Spark, published ports,
latest image tags, and a writable project mount.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "$#" -ne 0 ]]; then
  usage >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found in PATH." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed, but the Docker daemon is not available." >&2
  exit 1
fi

if [[ ! -f "${SCENARIO_PATH}" ]]; then
  echo "Scenario file not found: ${SCENARIO_PATH}" >&2
  exit 1
fi

ensure_image() {
  local image="$1"
  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo "Docker image already available: ${image}"
  else
    echo "Pulling Docker image: ${image}"
    docker pull "${image}"
  fi
}

container_exists() {
  docker container inspect "$1" >/dev/null 2>&1
}

container_is_running() {
  [[ "$(docker container inspect --format '{{.State.Running}}' "$1")" == "true" ]]
}

verify_demo_container() {
  local container="$1"
  local component="$2"
  local label
  local version
  label="$(docker container inspect --format "{{index .Config.Labels \"${DEMO_LABEL}\"}}" "${container}")"
  if [[ "${label}" != "${component}" ]]; then
    echo "Container name ${container} is already used by something other than this ${component} demo." >&2
    exit 1
  fi
  version="$(docker container inspect --format "{{index .Config.Labels \"${DEMO_VERSION_LABEL}\"}}" "${container}")"
  if [[ "${version}" != "${DEMO_VERSION}" ]]; then
    echo "Container ${container} belongs to an older demo configuration." >&2
    echo "Remove that demo container and run this script again to apply the current mounts and environment." >&2
    exit 1
  fi
}

start_existing_container() {
  local container="$1"
  local component="$2"
  verify_demo_container "${container}" "${component}"
  if container_is_running "${container}"; then
    echo "Container already running: ${container}"
  else
    docker start "${container}" >/dev/null
    echo "Started existing container: ${container}"
  fi
}

wait_for_vault() {
  local attempt
  for attempt in $(seq 1 30); do
    if docker exec \
      --env VAULT_ADDR=http://127.0.0.1:8200 \
      --env VAULT_TOKEN="${VAULT_DEV_TOKEN}" \
      "${VAULT_CONTAINER}" vault status >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Vault did not become ready. Inspect it with: docker logs ${VAULT_CONTAINER}" >&2
  exit 1
}

ensure_image "${VAULT_IMAGE}"
ensure_image "${JUPYTER_IMAGE}"

if ! docker network inspect "${DEMO_NETWORK}" >/dev/null 2>&1; then
  docker network create "${DEMO_NETWORK}" >/dev/null
  echo "Created Docker network: ${DEMO_NETWORK}"
fi

if container_exists "${VAULT_CONTAINER}"; then
  start_existing_container "${VAULT_CONTAINER}" "vault"
else
  docker run --detach \
    --name "${VAULT_CONTAINER}" \
    --label "${DEMO_LABEL}=vault" \
    --label "${DEMO_VERSION_LABEL}=${DEMO_VERSION}" \
    --network "${DEMO_NETWORK}" \
    --cap-add IPC_LOCK \
    --publish "${VAULT_PORT}:8200" \
    --env VAULT_DEV_ROOT_TOKEN_ID="${VAULT_DEV_TOKEN}" \
    --env VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200 \
    --mount "type=bind,source=${SCENARIO_PATH},target=/demo/personal_health_records_scenario.json,readonly" \
    "${VAULT_IMAGE}" server -dev >/dev/null
  echo "Created Vault container: ${VAULT_CONTAINER}"
fi

wait_for_vault

docker exec \
  --env VAULT_ADDR=http://127.0.0.1:8200 \
  --env VAULT_TOKEN="${VAULT_DEV_TOKEN}" \
  "${VAULT_CONTAINER}" vault kv put "${VAULT_SECRET_PATH}" \
  scenario=@/demo/personal_health_records_scenario.json >/dev/null
echo "Stored the personal-health scenario at Vault KV path: ${VAULT_SECRET_PATH}"

if container_exists "${JUPYTER_CONTAINER}"; then
  start_existing_container "${JUPYTER_CONTAINER}" "jupyter"
else
  docker run --detach \
    --name "${JUPYTER_CONTAINER}" \
    --label "${DEMO_LABEL}=jupyter" \
    --label "${DEMO_VERSION_LABEL}=${DEMO_VERSION}" \
    --network "${DEMO_NETWORK}" \
    --publish "${JUPYTER_PORT}:8888" \
    --publish "${SPARK_UI_PORT}:4040" \
    --env VAULT_ADDR="http://${VAULT_CONTAINER}:8200" \
    --env VAULT_TOKEN="${VAULT_DEV_TOKEN}" \
    --env KEBAB_STORM_SCENARIO_SECRET="${VAULT_SECRET_PATH}" \
    --env KEBAB_STORM_ALLOW_INSECURE_VAULT=true \
    --env KEBAB_STORM_DATA_LOCATION="${CONTAINER_PROJECT_PATH}/demo/output" \
    --env KEBAB_STORM_STORAGE_PROVIDER=parquet \
    --mount "type=bind,source=${PROJECT_ROOT},target=${CONTAINER_PROJECT_PATH}" \
    "${JUPYTER_IMAGE}" start-notebook.py \
    --ServerApp.token="${JUPYTER_TOKEN}" \
    --ServerApp.root_dir=/home/jovyan/work >/dev/null
  echo "Created Jupyter container: ${JUPYTER_CONTAINER}"
fi

if [[ -f "${ANALYTICS_PACKAGE_PATH}/pyproject.toml" ]]; then
  docker exec "${JUPYTER_CONTAINER}" \
    python -m pip install --editable "${CONTAINER_ANALYTICS_PACKAGE_PATH}"
  docker exec "${JUPYTER_CONTAINER}" bash -lc \
    'source /usr/local/bin/before-notebook.d/10spark-config.sh && python -c '\''import pyspark; import kebab_storm_analytics; print("PySpark " + pyspark.__version__ + ", Kebab Storm Analytics " + kebab_storm_analytics.__version__)'\'''
  echo "Installed and verified the mounted Kebab Storm Analytics package in Jupyter."
else
  echo "Notebook package not found: ${ANALYTICS_PACKAGE_PATH}" >&2
  exit 1
fi

echo
echo "Local Docker demonstration is ready:"
echo "  Vault UI:  http://localhost:${VAULT_PORT}/ui"
echo "  Vault token: ${VAULT_DEV_TOKEN}"
echo "  Vault secret: ${VAULT_SECRET_PATH}"
echo "  JupyterLab: http://localhost:${JUPYTER_PORT}/lab?token=${JUPYTER_TOKEN}"
echo "  Spark UI:   http://localhost:${SPARK_UI_PORT} (available while a Spark session is running)"
echo
echo "Import the health demo first with ./demo/health/01_import.sh if demo/output has no health dataset."
echo "WARNING: This is an insecure local demonstration, not a production deployment template."
echo "It uses development tokens, HTTP Vault, local Spark, latest images, and a writable project mount."
echo "Never use this environment for production, personal, health, financial, or otherwise sensitive data."
