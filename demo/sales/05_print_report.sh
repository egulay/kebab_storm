#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/demo_env.sh"
cd "${PROJECT_ROOT}"

"${DEMO_PYTHON}" -m reporter.simple_sales_report.print_simple_sales_report \
  --scenario "${DEMO_SCENARIO}" \
  --date "${DEMO_REPORT_DATE}"
