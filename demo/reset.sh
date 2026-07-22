#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

remove_demo_directory() {
  local target="$1"
  local expected_name="$2"
  local expected_target="${DEMO_DIR}/${expected_name}"

  if [[ "${target}" != "${expected_target}" ]]; then
    echo "Refusing to remove unexpected path: ${target}" >&2
    exit 1
  fi

  if [[ -e "${target}" || -L "${target}" ]]; then
    rm -rf -- "${target}"
    echo "Removed ${target}"
  else
    echo "Already clean: ${target}"
  fi
}

# Sales and health demos share these default locations. Removing them clears
# imported data, generated reports, and application logs.
remove_demo_directory "${DEMO_DIR}/output" "output"
remove_demo_directory "${DEMO_DIR}/logs" "logs"

echo "Kebab Storm demo state has been reset."
