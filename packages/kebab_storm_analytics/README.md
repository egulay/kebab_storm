# Kebab Storm Analytics

`kebab-storm-analytics` is a notebook SDK with a read-only public data-access API for Kebab Storm. It retrieves and validates a scenario from
HashiCorp Vault, reads the scenario's encrypted Parquet or Iceberg dataset, and returns a lazy Spark DataFrame. The
caller can keep all protected fields encrypted, decrypt every protected field defined by the scenario, or decrypt only
an explicitly selected set of fields.

The SDK exposes no import, write, update, soft-delete, hard-delete, or report-persistence operations. Storage and Vault
identities must also be restricted to read-only permissions by the deployment platform.

**The supplied Docker/Jupyter environment is an insecure local demonstration, not a production deployment template,
and must never process real sensitive data.** The unchanged SDK can still be used as a component of a governed
platform; see
[Production deployment path and security boundary](#production-deployment-path-and-security-boundary).

## Start a Spark session

The host Spark runtime must support the native `aes_decrypt` function. PySpark is an optional package dependency
because managed notebooks and Spark clusters normally provide their own matching PySpark version.

```python
import os

from pyspark.sql import SparkSession, functions as F
from kebab_storm_analytics import read_dataset

spark = (
    SparkSession.builder
    .appName("Kebab Storm Health Analytics")
    .getOrCreate()
)
```

## Connect to Vault explicitly

For a governed deployment, supply an HTTPS Vault address and obtain a short-lived token from the notebook service or
platform workload identity mechanism. These are minimum connection requirements, not a complete production security
design. Do not embed a token in a notebook, and review the production boundary below.

```python
encrypted_health = read_dataset(
    spark,
    vault_addr=os.environ["VAULT_ADDR"],  # production value must be an HTTPS URL
    vault_token=os.environ["VAULT_TOKEN"],
    vault_namespace=os.environ.get("VAULT_NAMESPACE"),  # optional; commonly used with Vault Enterprise
    vault_secret=os.environ["KEBAB_STORM_SCENARIO_SECRET"],
    data_location=os.environ["KEBAB_STORM_DATA_LOCATION"],
    storage_provider="parquet",
)
```

`crypto_action="encrypted"` is the default. In this mode, fields marked `is_encrypted: true` in the scenario remain
Spark binary columns. The SDK still applies the scenario's storage location and filters, but it does not expose their
plaintext values.

The local Docker demonstration uses HTTP only inside its private Docker network. Its equivalent explicit call is:

```python
demo_health = read_dataset(
    spark,
    vault_addr="http://kebab-storm-vault:8200",
    vault_token=os.environ["VAULT_TOKEN"],
    vault_secret="secret/kebab-storm/personal-health-records",
    allow_insecure_vault_http=True,  # local Docker demo only
    data_location="/home/jovyan/work/kebab_storm/demo/output",
    storage_provider="parquet",
)
```

The Docker launcher already sets these values as environment variables, so the same call can be shortened to:

```python
encrypted_health = read_dataset(spark)
```

## Decrypt only selected fields

Use mapped scenario names in `decrypt_columns`, not source CSV names. The following call decrypts exactly three fields;
every other protected field remains binary:

```python
health_for_diagnosis_analysis = read_dataset(
    spark,
    crypto_action="decrypted",
    decrypt_columns=[
        "BIOLOGICAL_SEX",
        "GLUCOSE_LEVEL",
        "DIAGNOSIS_CODE",
    ],
)

health_for_diagnosis_analysis.groupBy(
    "BIOLOGICAL_SEX",
    "DIAGNOSIS_CODE",
).agg(
    F.count("*").alias("PATIENT_COUNT"),
    F.round(F.avg("GLUCOSE_LEVEL"), 1).alias("AVERAGE_GLUCOSE"),
).orderBy("BIOLOGICAL_SEX", "DIAGNOSIS_CODE").show(truncate=False)
```

Selecting the minimum necessary fields reduces plaintext exposure in the notebook, Spark plan, and displayed output.
Passing an unknown, unencrypted, or source-name field in `decrypt_columns` raises an error instead of silently ignoring
it.

## Apply all decryption declared by the scenario

Set `crypto_action="decrypted"` and omit `decrypt_columns` to decrypt every field whose scenario definition has
`is_encrypted: true`:

```python
fully_decrypted_health = read_dataset(
    spark,
    crypto_action="decrypted",
)

fully_decrypted_health.printSchema()
```

For each protected field, the SDK uses the encryption key and mapped column name from the validated scenario, then
restores the scenario's declared Spark type. Unencrypted fields retain their stored types. This read operation does not
rerun source-file conversions: those conversions already occurred during import.

Decrypt-all is convenient for controlled exploration but exposes more plaintext than selective decryption. Prefer
`decrypt_columns` for normal analytical work.

## Filter imported partitions and soft-deleted records

Soft-deleted rows are excluded by default. Dates use `YYYY-MM-DD` and are inclusive:

```python
july_health = read_dataset(
    spark,
    crypto_action="decrypted",
    decrypt_columns=["BIOLOGICAL_SEX", "GLUCOSE_LEVEL"],
    start_date="2026-07-01",
    end_date="2026-07-31",
    include_soft_deleted=False,
)
```

An authorized audit notebook can include retained soft-deleted rows explicitly:

```python
health_with_soft_deleted = read_dataset(
    spark,
    start_date="2026-07-01",
    end_date="2026-07-31",
    include_soft_deleted=True,
)
```

That example intentionally leaves protected fields encrypted because it does not request decryption.

## Read an Iceberg-backed dataset

The Spark session must already contain the Iceberg runtime and catalog configuration. The SDK derives the table name
from the scenario's `import_save_location`, using the same catalog and namespace used by the importer.

```python
iceberg_health = read_dataset(
    spark,
    vault_addr=os.environ["VAULT_ADDR"],  # production value must be an HTTPS URL
    vault_token=os.environ["VAULT_TOKEN"],
    vault_secret=os.environ["KEBAB_STORM_SCENARIO_SECRET"],
    data_location=os.environ["KEBAB_STORM_DATA_LOCATION"],
    storage_provider="iceberg",
    iceberg_catalog="kebab",
    iceberg_namespace="default",
    crypto_action="decrypted",
    decrypt_columns=["BIOLOGICAL_SEX", "GLUCOSE_LEVEL"],
)
```

## Environment-based configuration

Every deployment setting can be supplied as a `read_dataset()` argument or through the notebook service environment:

- `VAULT_ADDR`: absolute Vault URL; HTTPS is required outside the local demo
- `VAULT_TOKEN`: token with read access only to the required scenario secret
- `VAULT_NAMESPACE`: optional Vault namespace
- `KEBAB_STORM_SCENARIO_SECRET`: KV v2 secret path, for example
  `secret/kebab-storm/personal-health-records`
- `KEBAB_STORM_DATA_LOCATION`: storage root used by the importer
- `KEBAB_STORM_STORAGE_PROVIDER`: `parquet` or `iceberg`; defaults to `parquet`
- `KEBAB_STORM_ICEBERG_CATALOG`: Iceberg catalog; defaults to `kebab`
- `KEBAB_STORM_ICEBERG_NAMESPACE`: Iceberg namespace; defaults to `default`
- `KEBAB_STORM_ALLOW_INSECURE_VAULT`: accepts `true` only for the repository's local HTTP Vault demonstration

With those variables managed by the notebook platform, analytical code contains no infrastructure values:

```python
health = read_dataset(
    spark,
    crypto_action="decrypted",
    decrypt_columns=["BIOLOGICAL_SEX", "GLUCOSE_LEVEL"],
)
```

## Read-only security boundary

The SDK does not cache, collect, display, export, or write the returned DataFrame automatically. It also cannot prevent
a notebook user from calling Spark write operations directly, so the Jupyter or cluster service identity must have
read-only storage permissions and a narrowly scoped, read-only Vault policy. Notebook output, Spark UI details, event
logs, driver memory, executors, and anyone able to run notebook code are inside the trusted boundary whenever plaintext
is requested.

Vault HTTP is rejected by default. `KEBAB_STORM_ALLOW_INSECURE_VAULT=true` and
`allow_insecure_vault_http=True` exist exclusively for the local Docker demonstration.

## Production deployment path and security boundary

The SDK can be deployed without source-code changes as a component of a governed production notebook or Spark
platform. Install the same package in the approved Spark runtime; provide its existing Vault, storage, Iceberg, and
Spark settings through the platform; and call the same `read_dataset()` API. Authentication, authorization, network
security, read-only storage enforcement, auditing, dependency governance, monitoring, and recovery remain deployment
responsibilities rather than SDK-specific code.

This no-code-change path assumes Vault KV v2 over HTTPS with a platform-supplied short-lived token, supported Parquet
or Iceberg storage, a compatible Spark runtime, and scenario-level authorization. A user who receives a complete
scenario is trusted to request any encrypted field in it. Within that model, moving from the demo to production is a
configuration and platform-integration task; it does not require a fork, custom cryptography, or a Jupyter plugin.

The SDK itself is read-only only at its public API boundary. It cannot stop arbitrary notebook code from using Spark
write APIs, reading an available Vault token, retrieving a scenario directly, requesting all encrypted fields,
printing plaintext, or exporting results. Selective decryption is not an authorization system. Anyone allowed to run
code with the scenario credential and storage access belongs to the trusted data-access boundary; the SDK does not
protect against intentional extraction by that authorized user.

A real deployment must independently provide an authenticated and isolated notebook platform, short-lived
identity-based HTTPS Vault access, narrowly scoped Vault policies and audit logs, read-only storage ACLs, separate
ingestion identities, private networking and egress controls, protected Spark UI and event logs, output governance,
pinned and reviewed dependencies/images, vulnerability and patch management, monitoring, backup, recovery, incident
response, and all applicable privacy/compliance controls. Kebab Storm Analytics may be one component in that design;
this repository does not provide, certify, or replace the surrounding platform.

Requirements outside this trust model—such as per-field authorization inside one scenario, keys hidden from arbitrary
notebook code, HSM/KMS operations that never expose keys to Spark, prevention of intentional analyst extraction, or an
unsupported Vault protocol—would require a different architecture or code change.

## Run the local demonstration

**The included `demo/notebooks/start_jupyter_demo.sh` environment must never be used as a production notebook
deployment or process real sensitive data.** It uses Vault development mode over HTTP, fixed development tokens, host-published ports,
`latest` image tags, local Spark, a writable project bind mount, and public demonstration scenarios/keys. It does not
implement the production platform controls described above.

From the repository root:

```sh
./demo/health/01_import.sh
./demo/notebooks/start_jupyter_demo.sh
```

The first command creates the encrypted health dataset. The second starts Vault and Jupyter, loads the health scenario
into Vault, and installs this package in the notebook container. It does not import or modify analytical data. Open
the JupyterLab URL printed by the launcher, normally
`http://localhost:8888/lab?token=kebab-storm-demo-token`. In the JupyterLab file browser, open `kebab_storm`, then
`demo`, then `notebooks`, and double-click `personal_health_analytics.ipynb`. Run its cells from top to bottom. The
notebook demonstrates the encrypted default, selective decryption and aggregation, an illustrative Spark ML regression
with a Matplotlib evaluation chart, and scenario-directed full decryption without requesting personal identifiers. The
synthetic regression is not a clinically meaningful model. The Spark UI is available at `http://localhost:4040` while
its Spark session is active.

Reset the complete local demonstration from the repository root with:

```sh
./demo/reset.sh
```

This removes generated demo data and logs, Jupyter checkpoint directories under the project root and
`demo/notebooks`, and the labelled Jupyter and Vault demo containers. It preserves the checked-in notebooks, README,
scenarios, source files, Docker images, and Docker network.
