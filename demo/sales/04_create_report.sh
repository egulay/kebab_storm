#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/demo_env.sh"
cd "${PROJECT_ROOT}"

"${DEMO_PYTHON}" -m reporter.simple_sales_report.create_simple_sales_report \
  --scenario "${DEMO_SCENARIO}" \
  --include-soft-deleted "${DEMO_INCLUDE_SOFT_DELETED}" \
  --date "${DEMO_IMPORT_DATE}"
