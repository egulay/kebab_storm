#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"
VAULT_CONTAINER="${VAULT_CONTAINER:-kebab-storm-vault}"
JUPYTER_CONTAINER="${JUPYTER_CONTAINER:-kebab-storm-jupyter}"
DEMO_LABEL="io.kebab-storm.demo"

remove_demo_directory() {
  local target="$1"

  case "${target}" in
    "${DEMO_DIR}/output" | \
    "${DEMO_DIR}/logs" | \
    "${DEMO_DIR}/notebooks/.ipynb_checkpoints" | \
    "${PROJECT_ROOT}/.ipynb_checkpoints")
      ;;
    *)
      echo "Refusing to remove unexpected path: ${target}" >&2
      exit 1
      ;;
  esac

  if [[ -e "${target}" || -L "${target}" ]]; then
    rm -rf -- "${target}"
    echo "Removed ${target}"
  else
    echo "Already clean: ${target}"
  fi
}

remove_demo_container() {
  local container="$1"
  local expected_component="$2"
  local actual_component

  if ! docker container inspect "${container}" >/dev/null 2>&1; then
    echo "Already clean: Docker container ${container}"
    return
  fi

  actual_component="$(docker container inspect \
    --format "{{index .Config.Labels \"${DEMO_LABEL}\"}}" \
    "${container}")"
  if [[ "${actual_component}" != "${expected_component}" ]]; then
    echo "Refusing to remove container without the expected Kebab Storm demo label: ${container}" >&2
    return 1
  fi

  docker container rm --force "${container}" >/dev/null
  echo "Removed Docker container ${container}"
}

reset_notebook_demo() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is not available; skipped Vault and Jupyter container cleanup."
    return
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not available; skipped Vault and Jupyter container cleanup."
    return
  fi

  remove_demo_container "${JUPYTER_CONTAINER}" "jupyter"
  remove_demo_container "${VAULT_CONTAINER}" "vault"
}

# Reset the notebook services, then clear the default locations shared by the
# sales and health demonstrations and any generated notebook checkpoints.
reset_notebook_demo
remove_demo_directory "${DEMO_DIR}/output"
remove_demo_directory "${DEMO_DIR}/logs"
remove_demo_directory "${DEMO_DIR}/notebooks/.ipynb_checkpoints"
remove_demo_directory "${PROJECT_ROOT}/.ipynb_checkpoints"

echo "Kebab Storm demo state has been reset."
