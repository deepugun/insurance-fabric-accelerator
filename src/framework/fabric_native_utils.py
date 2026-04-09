# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Fabric Native Utilities
# Wrapper around Fabric built-in features: notebookutils, mssparkutils,
# Fabric REST APIs, OneLake, Lakehouse, Eventstream, Activator.
# ALL components MUST use this module — never reimplement Fabric features.
# ──────────────────────────────────────────────────────────────────────────────
#
# FABRIC BUILT-IN FEATURES USED (DO NOT REIMPLEMENT):
#
# ┌─────────────────────────────────┬──────────────────────────────────────────┐
# │ Feature                         │ Fabric Built-In                          │
# ├─────────────────────────────────┼──────────────────────────────────────────┤
# │ Secret management               │ notebookutils.credentials.getSecret()   │
# │ Token acquisition                │ notebookutils.credentials.getToken()    │
# │ Lakehouse file I/O               │ notebookutils.fs (mount, cp, ls, rm)    │
# │ Notebook orchestration           │ notebookutils.notebook.run()            │
# │ Pipeline orchestration           │ notebookutils.pipeline.run()            │
# │ Email notification               │ notebookutils.notification.sendMail()   │
# │ Lakehouse management             │ notebookutils.lakehouse                 │
# │ Delta table OPTIMIZE             │ OPTIMIZE table_name                     │
# │ Delta table VACUUM               │ VACUUM table_name                       │
# │ Delta table versioning           │ DESCRIBE HISTORY table_name             │
# │ Delta time travel                │ SELECT * FROM t VERSION AS OF n         │
# │ Schema evolution                 │ .option("mergeSchema", "true")          │
# │ Deployment pipelines             │ Fabric REST API /deploymentPipelines    │
# │ Git integration                  │ Fabric REST API /git                    │
# │ Workspace management             │ Fabric REST API /workspaces             │
# │ Capacity management              │ Fabric REST API /capacities             │
# │ Domain management                │ Fabric REST API /admin/domains          │
# │ Eventstream (real-time)          │ Fabric Eventstream item                 │
# │ Activator (alerts)              │ Fabric Reflex / Data Activator          │
# │ Direct Lake semantic models     │ Fabric Semantic Model (Direct Lake)     │
# │ Monitoring                       │ Fabric REST API /admin/monitoring       │
# │ Data Quality                     │ Spark DQ checks + Delta constraints    │
# │ RLS / CLS                        │ Power BI / Semantic Model security     │
# │ Managed Private Endpoints       │ Fabric Managed VNet                     │
# │ Workspace Identity               │ Fabric Workspace Identity (MSI)        │
# │ OneLake shortcuts                │ Fabric Shortcuts API                    │
# │ Purview lineage                  │ Automatic via Fabric-Purview           │
# │ MLflow tracking                  │ Built-in Fabric MLflow                 │
# │ Spark session management         │ Built-in Fabric Spark pools            │
# └─────────────────────────────────┴──────────────────────────────────────────┘
#
# ──────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
import json
import requests
from typing import Optional, Dict, List, Any

spark = SparkSession.builder.getOrCreate()


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC ENVIRONMENT DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def is_fabric_environment() -> bool:
    """Detect if running inside a Fabric notebook."""
    try:
        from notebookutils import mssparkutils  # noqa: F401
        return True
    except ImportError:
        return False

IS_FABRIC = is_fabric_environment()


# ═══════════════════════════════════════════════════════════════════════════════
# SECRETS — Use notebookutils.credentials (NEVER roll your own)
# ═══════════════════════════════════════════════════════════════════════════════

def get_secret(key_vault_url: str, secret_name: str) -> str:
    """Retrieve secret from Azure Key Vault using Fabric built-in credentials."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        return mssparkutils.credentials.getSecret(key_vault_url, secret_name)
    else:
        # Local dev fallback — use environment variables
        import os
        return os.environ.get(secret_name, f"dev-placeholder-{secret_name}")


def get_token(resource: str = "https://api.fabric.microsoft.com") -> str:
    """Get OAuth token using Fabric Workspace Identity (Managed Identity)."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        return mssparkutils.credentials.getToken(resource)
    else:
        # Local dev: use Azure CLI token
        import subprocess
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True
        )
        return result.stdout.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC REST API CLIENT — Built on top of Workspace Identity
# ═══════════════════════════════════════════════════════════════════════════════

class FabricAPIClient:
    """
    Thin wrapper around Fabric REST API.
    Uses Fabric Workspace Identity (Managed Identity) for auth — no SP needed.
    """
    BASE_URL = "https://api.fabric.microsoft.com/v1"
    ADMIN_URL = "https://api.fabric.microsoft.com/v1/admin"

    def __init__(self):
        self._token = None

    @property
    def token(self) -> str:
        if not self._token:
            self._token = get_token("https://api.fabric.microsoft.com")
        return self._token

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get(self, path: str, params: dict = None) -> dict:
        resp = requests.get(f"{self.BASE_URL}{path}", headers=self.headers,
                            params=params, timeout=60)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def post(self, path: str, body: dict = None) -> dict:
        resp = requests.post(f"{self.BASE_URL}{path}", headers=self.headers,
                             json=body or {}, timeout=120)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def patch(self, path: str, body: dict = None) -> dict:
        resp = requests.patch(f"{self.BASE_URL}{path}", headers=self.headers,
                              json=body or {}, timeout=60)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def delete(self, path: str) -> None:
        resp = requests.delete(f"{self.BASE_URL}{path}", headers=self.headers,
                               timeout=60)
        resp.raise_for_status()

    def admin_get(self, path: str, params: dict = None) -> dict:
        resp = requests.get(f"{self.ADMIN_URL}{path}", headers=self.headers,
                            params=params, timeout=60)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def admin_post(self, path: str, body: dict = None) -> dict:
        resp = requests.post(f"{self.ADMIN_URL}{path}", headers=self.headers,
                             json=body or {}, timeout=120)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    # ── Convenience: Long-running operations ──
    def post_long_running(self, path: str, body: dict = None,
                          poll_seconds: int = 5, max_polls: int = 60) -> dict:
        """POST that returns 202 with Operation-Location header."""
        resp = requests.post(f"{self.BASE_URL}{path}", headers=self.headers,
                             json=body or {}, timeout=120)
        if resp.status_code == 202:
            op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
            if op_url:
                import time
                for _ in range(max_polls):
                    time.sleep(poll_seconds)
                    poll = requests.get(op_url, headers=self.headers, timeout=60)
                    result = poll.json()
                    status = result.get("status", "")
                    if status in ("Succeeded", "succeeded", "Completed"):
                        return result
                    if status in ("Failed", "failed"):
                        raise RuntimeError(f"Long-running operation failed: {result}")
        resp.raise_for_status()
        return resp.json() if resp.content else {}


# Singleton
fabric_api = FabricAPIClient()


# ═══════════════════════════════════════════════════════════════════════════════
# FILE OPERATIONS — Use notebookutils.fs (OneLake native)
# ═══════════════════════════════════════════════════════════════════════════════

def onelake_ls(path: str) -> List[str]:
    """List files in OneLake path using Fabric built-in fs."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        return [f.name for f in mssparkutils.fs.ls(path)]
    else:
        # Spark fallback
        try:
            from py4j.protocol import Py4JJavaError
            hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            )
            status = hadoop_fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
            return [s.getPath().getName() for s in status]
        except Exception:
            return []


def onelake_cp(src: str, dst: str, recurse: bool = False):
    """Copy files in OneLake using Fabric built-in fs."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        mssparkutils.fs.cp(src, dst, recurse)
    else:
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileUtil.copy(
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()),
            spark._jvm.org.apache.hadoop.fs.Path(src),
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()),
            spark._jvm.org.apache.hadoop.fs.Path(dst),
            False, spark._jsc.hadoopConfiguration()
        )


def onelake_rm(path: str, recurse: bool = False):
    """Remove files in OneLake using Fabric built-in fs."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        mssparkutils.fs.rm(path, recurse)


def onelake_mkdirs(path: str):
    """Create directory in OneLake using Fabric built-in fs."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        mssparkutils.fs.mkdirs(path)


# ═══════════════════════════════════════════════════════════════════════════════
# NOTEBOOK ORCHESTRATION — Use notebookutils.notebook (built-in)
# ═══════════════════════════════════════════════════════════════════════════════

def run_notebook(notebook_name: str, parameters: dict = None,
                 timeout_seconds: int = 600) -> str:
    """Run a Fabric notebook using built-in orchestration."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        return mssparkutils.notebook.run(
            notebook_name,
            timeout_seconds,
            parameters or {}
        )
    else:
        print(f"[DEV] Would run notebook: {notebook_name} with params: {parameters}")
        return "dev-skip"


def run_notebook_parallel(notebook_configs: List[Dict]) -> List:
    """Run multiple notebooks in parallel using Fabric built-in parallelism."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        dag = {
            "activities": [
                {
                    "name": cfg["name"],
                    "path": cfg["notebook"],
                    "args": cfg.get("parameters", {}),
                    "timeoutPerCellInSeconds": cfg.get("timeout", 600),
                    "retry": cfg.get("retry", 0),
                }
                for cfg in notebook_configs
            ],
            "timeoutInSeconds": max(c.get("timeout", 600) for c in notebook_configs) * 2,
            "concurrency": len(notebook_configs),
        }
        return mssparkutils.notebook.runMultiple(dag)
    else:
        return [{"name": c["name"], "status": "dev-skip"} for c in notebook_configs]


# ═══════════════════════════════════════════════════════════════════════════════
# NOTIFICATIONS — Use notebookutils.notification (built-in)
# ═══════════════════════════════════════════════════════════════════════════════

def send_email(to: str, subject: str, body: str):
    """Send email using Fabric built-in notification."""
    if IS_FABRIC:
        from notebookutils import mssparkutils
        mssparkutils.notification.sendMail(
            to=to,
            subject=subject,
            body=body
        )
    else:
        print(f"[DEV] Email → {to}: {subject}")


# ═══════════════════════════════════════════════════════════════════════════════
# DELTA TABLE MAINTENANCE — Use built-in OPTIMIZE & VACUUM
# ═══════════════════════════════════════════════════════════════════════════════

def optimize_table(table_name: str, z_order_cols: List[str] = None):
    """Run Fabric-native OPTIMIZE (built-in V-Order + Delta optimization)."""
    if z_order_cols:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({','.join(z_order_cols)})")
    else:
        # Fabric applies V-Order automatically on OPTIMIZE
        spark.sql(f"OPTIMIZE {table_name}")


def vacuum_table(table_name: str, retention_hours: int = 168):
    """Run Fabric-native VACUUM for storage cleanup."""
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")


def describe_table_history(table_name: str, limit: int = 20):
    """Get Delta table version history (built-in)."""
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def time_travel_query(table_name: str, version: int):
    """Query a specific table version (built-in Delta time travel)."""
    return spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {version}")


def add_table_constraint(table_name: str, constraint_name: str, expression: str):
    """Add Delta table constraint (built-in data quality)."""
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} CHECK ({expression})")


# ═══════════════════════════════════════════════════════════════════════════════
# LAKEHOUSE MANAGEMENT — Via Fabric REST API
# ═══════════════════════════════════════════════════════════════════════════════

def create_lakehouse(workspace_id: str, display_name: str,
                     description: str = "") -> dict:
    """Create a Lakehouse using Fabric REST API."""
    return fabric_api.post_long_running(
        f"/workspaces/{workspace_id}/lakehouses",
        {"displayName": display_name, "description": description}
    )


def create_shortcut(workspace_id: str, lakehouse_id: str, shortcut_name: str,
                    target_path: str, source_type: str = "OneLake"):
    """Create OneLake shortcut (built-in cross-lakehouse reference)."""
    body = {
        "name": shortcut_name,
        "path": "Tables",
        "target": {}
    }
    if source_type == "OneLake":
        body["target"]["oneLake"] = {
            "path": target_path,
        }
    elif source_type == "adlsGen2":
        body["target"]["adlsGen2"] = {
            "location": target_path,
        }

    return fabric_api.post(
        f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",
        body
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC DOMAIN MANAGEMENT — Built-in Fabric Domains
# ═══════════════════════════════════════════════════════════════════════════════

def create_fabric_domain(domain_name: str, description: str = "",
                         parent_domain_id: str = None) -> dict:
    """Create a Fabric Domain using admin API (built-in feature)."""
    body = {
        "displayName": domain_name,
        "description": description,
    }
    if parent_domain_id:
        body["parentDomainId"] = parent_domain_id
    return fabric_api.admin_post("/domains", body)


def assign_workspace_to_domain(domain_id: str, workspace_ids: List[str]):
    """Assign workspaces to a Fabric Domain (built-in governance)."""
    return fabric_api.admin_post(
        f"/domains/{domain_id}/assignWorkspaces",
        {"workspacesIds": workspace_ids}
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SPARK SESSION CONFIG — Fabric Spark built-in settings
# ═══════════════════════════════════════════════════════════════════════════════

def configure_spark_for_insurance():
    """Apply optimal Spark settings for insurance workloads on Fabric."""
    # These are Fabric-native settings — no custom Spark pool config needed
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")          # V-Order (Fabric)
    spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Optimize Write
    spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")  # 1GB bins
    spark.conf.set("spark.sql.shuffle.partitions", "auto")              # Auto shuffle
    spark.conf.set("spark.sql.adaptive.enabled", "true")                # AQE
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    # Enable Fabric High Concurrency mode settings
    spark.conf.set("spark.microsoft.delta.merge.lowShuffle.enabled", "true")


# ═══════════════════════════════════════════════════════════════════════════════
# ONELAKE PATH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_onelake_tables_path(workspace_name: str, lakehouse_name: str) -> str:
    """Get the abfss path for lakehouse Tables (Delta tables)."""
    return (f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse_name}.Lakehouse/Tables")


def get_onelake_files_path(workspace_name: str, lakehouse_name: str) -> str:
    """Get the abfss path for lakehouse Files (unstructured)."""
    return (f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse_name}.Lakehouse/Files")


# Initialize Spark config on import
configure_spark_for_insurance()
print("✅ Fabric Native Utilities loaded. IS_FABRIC =", IS_FABRIC)
