#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/demo_env.sh"
cd "${PROJECT_ROOT}"

"${DEMO_PYTHON}" -m reporter.personal_health_report.print_personal_health_report \
  --scenario "${DEMO_SCENARIO}" \
  --date "${DEMO_REPORT_DATE}"
