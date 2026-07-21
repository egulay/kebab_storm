#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PROJECT_ROOT="$(cd "${DEMO_DIR}/../.." && pwd)"

export KEBAB_STORM_ENV="${KEBAB_STORM_ENV:-local}"
export KEBAB_STORM_SPARK_MASTER="${KEBAB_STORM_SPARK_MASTER:-local[*]}"
export KEBAB_STORM_DATA_LOCATION="${KEBAB_STORM_DATA_LOCATION:-${PROJECT_ROOT}/demo/output}"
export KEBAB_STORM_LOGGING_LOCATION="${KEBAB_STORM_LOGGING_LOCATION:-${PROJECT_ROOT}/demo/logs}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export TZ="${TZ:-UTC}"
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

export DEMO_PYTHON="${DEMO_PYTHON:-${PROJECT_ROOT}/.venv/bin/python}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${DEMO_PYTHON}}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${DEMO_PYTHON}}"

export DEMO_SCENARIO="${DEMO_SCENARIO:-${PROJECT_ROOT}/scenario/personal_health_records_scenario.json}"
export DEMO_INPUT_FILE="${DEMO_INPUT_FILE:-${PROJECT_ROOT}/data/50k_personal_health_records.csv}"
export DEMO_IMPORT_DATE="${DEMO_IMPORT_DATE:-2026-07-21}"
export DEMO_REPORT_DATE="${DEMO_REPORT_DATE:-$(date +%Y-%m-%d)}"
export DEMO_INCLUDE_SOFT_DELETED="${DEMO_INCLUDE_SOFT_DELETED:-no}"

if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
  export JAVA_HOME="$(/usr/libexec/java_home)"
elif [[ -n "${JAVA_HOME:-}" ]]; then
  export JAVA_HOME
elif ! command -v java >/dev/null 2>&1; then
  echo "Java 17 or newer is required. Set JAVA_HOME before running this demo." >&2
  exit 1
fi

if [[ ! -x "${DEMO_PYTHON}" ]]; then
  echo "Python environment not found at ${DEMO_PYTHON}. Set DEMO_PYTHON to the project interpreter." >&2
  exit 1
fi

if [[ ! -f "${DEMO_SCENARIO}" || ! -f "${DEMO_INPUT_FILE}" ]]; then
  echo "Health demo scenario or input file was not found." >&2
  exit 1
fi

mkdir -p "${KEBAB_STORM_DATA_LOCATION}" "${KEBAB_STORM_LOGGING_LOCATION}"
