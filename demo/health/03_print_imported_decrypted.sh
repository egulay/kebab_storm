#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/demo_env.sh"
cd "${PROJECT_ROOT}"

"${DEMO_PYTHON}" -m etl.executor.print_imported \
  --scenario "${DEMO_SCENARIO}" \
  --crypto-action decrypted \
  --date "${DEMO_IMPORT_DATE}"
