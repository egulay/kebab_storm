#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/demo_env.sh"
cd "${PROJECT_ROOT}"

"${DEMO_PYTHON}" -m etl.executor.import \
  --scenario "${DEMO_SCENARIO}" \
  --input-file "${DEMO_INPUT_FILE}" \
  --date "${DEMO_IMPORT_DATE}"
