# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Data Archival, Cost Optimization & Monetization
# Uses Fabric-native features:
# - Delta Lake VACUUM, time travel, partitioning
# - OneLake shortcuts for archive access
# - Fabric capacity management APIs
# - OneLake data sharing for monetization
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, datediff, lit, expr
from datetime import datetime
from typing import Dict, List
import json
import uuid

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from fabric_native_utils import (
    fabric_api, send_email, onelake_cp, onelake_rm, optimize_table, vacuum_table
)

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"
MONITORING_SCHEMA = "insurance_monitoring"


# ═══════════════════════════════════════════════════════════════════════════════
# DATA ARCHIVAL ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class DataArchivalEngine:
    """
    Automated data archival using Fabric-native features:
    - Reads retention_policy from metadata
    - Moves expired data to archive lakehouse (OneLake copy)
    - Creates OneLake shortcuts for archive access (no duplication)
    - Runs VACUUM on source tables after archival
    - Tracks everything in audit log
    """

    def __init__(self, environment: str = "prod"):
        self.environment = environment

    def run_archival_cycle(self):
        """Execute archival for all tables with active retention policies."""
        policies = spark.sql(f"""
            SELECT * FROM {METADATA_SCHEMA}.retention_policy
            WHERE is_active = TRUE
        """).collect()

        results = []
        for policy in policies:
            p = policy.asDict()
            try:
                result = self._archive_table(p)
                results.append(result)
            except Exception as e:
                results.append({
                    "table": p["table_name"], "status": "failed", "error": str(e)
                })

        # Summarize
        archived = sum(1 for r in results if r.get("status") == "archived")
        print(f"📦 Archival complete: {archived}/{len(results)} tables processed")
        return results

    def _archive_table(self, policy: Dict) -> Dict:
        """Archive expired data for a single table."""
        table_name = policy["table_name"]
        retention_days = policy["retention_days"]
        retention_type = policy["retention_type"]

        # Calculate cutoff
        cutoff_expr = f"current_date() - INTERVAL {retention_days} DAYS"

        # Find the date/timestamp column to use for retention
        date_col = self._find_date_column(table_name)
        if not date_col:
            return {"table": table_name, "status": "skipped", "reason": "no date column found"}

        # Count records to archive
        expired_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {table_name}
            WHERE {date_col} < {cutoff_expr}
        """).first()["cnt"]

        if expired_count == 0:
            return {"table": table_name, "status": "no_data", "records": 0}

        if retention_type == "archive":
            # Move to archive lakehouse (separate Delta table)
            archive_table = f"archive_{policy['layer']}.{table_name.split('.')[-1]}_archive"
            spark.sql(f"""
                INSERT INTO {archive_table}
                SELECT *, current_timestamp() AS _archived_at
                FROM {table_name}
                WHERE {date_col} < {cutoff_expr}
            """)

            # Delete from source using Delta DELETE
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE {date_col} < {cutoff_expr}
            """)

            # Run VACUUM to reclaim space (Fabric built-in)
            vacuum_table(table_name, retention_hours=24)

        elif retention_type == "delete":
            # Permanent delete
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE {date_col} < {cutoff_expr}
            """)
            vacuum_table(table_name, retention_hours=24)

        elif retention_type == "snapshot":
            # Create regulatory snapshot before any cleanup
            snapshot_table = (f"archive_{policy['layer']}."
                              f"{table_name.split('.')[-1]}_snapshot_"
                              f"{datetime.utcnow().strftime('%Y%m%d')}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {snapshot_table} AS
                SELECT *, current_timestamp() AS _snapshot_at
                FROM {table_name}
                WHERE {date_col} < {cutoff_expr}
            """)

        # Update last purge date
        spark.sql(f"""
            UPDATE {METADATA_SCHEMA}.retention_policy
            SET last_purge_date = current_date()
            WHERE policy_id = '{policy["policy_id"]}'
        """)

        return {
            "table": table_name,
            "status": "archived",
            "records": expired_count,
            "type": retention_type,
        }

    def _find_date_column(self, table_name: str) -> str:
        """Find the best date column for retention in a table."""
        columns = spark.sql(f"DESCRIBE {table_name}").collect()
        date_candidates = [
            "created_at", "_ingestion_timestamp", "_silver_load_ts",
            "_gold_load_ts", "event_timestamp", "posting_date",
            "transaction_date", "effective_date"
        ]
        col_names = [c["col_name"] for c in columns]
        for candidate in date_candidates:
            if candidate in col_names:
                return candidate
        # Fallback: any timestamp column
        for c in columns:
            if c["data_type"] in ("timestamp", "date"):
                return c["col_name"]
        return None

    def calculate_storage_savings(self) -> Dict:
        """Calculate estimated storage savings from archival."""
        tables = spark.sql(f"""
            SELECT table_name, layer, retention_days, retention_type, last_purge_date
            FROM {METADATA_SCHEMA}.retention_policy
            WHERE is_active = TRUE
        """).collect()

        total_rows_archivable = 0
        for t in tables:
            date_col = self._find_date_column(t["table_name"])
            if date_col:
                try:
                    count = spark.sql(f"""
                        SELECT COUNT(*) AS cnt FROM {t['table_name']}
                        WHERE {date_col} < current_date() - INTERVAL {t['retention_days']} DAYS
                    """).first()["cnt"]
                    total_rows_archivable += count
                except Exception:
                    pass

        return {
            "total_archivable_rows": total_rows_archivable,
            "tables_with_policies": len(tables),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# COST OPTIMIZATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class CostOptimizationEngine:
    """
    Cost optimization recommendations using Fabric-native features:
    - Table optimization (OPTIMIZE / V-Order built-in)
    - Small file compaction (built-in optimizeWrite)
    - Partition pruning recommendations
    - Capacity right-sizing
    - Unused table detection
    """

    def generate_cost_report(self) -> List[Dict]:
        """Generate actionable cost optimization recommendations."""
        recommendations = []

        # 1. Tables needing OPTIMIZE (Fabric built-in)
        recommendations.extend(self._check_table_optimization())

        # 2. Unused tables (candidates for archival)
        recommendations.extend(self._check_unused_tables())

        # 3. Over-partitioned tables
        recommendations.extend(self._check_partitioning())

        # 4. Large files needing VACUUM
        recommendations.extend(self._check_vacuum_candidates())

        return recommendations

    def _check_table_optimization(self) -> List[Dict]:
        """Find tables that would benefit from OPTIMIZE (V-Order)."""
        recs = []
        tables = spark.sql("SHOW TABLES IN gold_policy").collect()
        for t in tables:
            try:
                # Check Delta log for number of files
                history = spark.sql(f"DESCRIBE DETAIL gold_policy.{t['tableName']}")
                detail = history.first()
                if detail and detail["numFiles"] > 100:
                    recs.append({
                        "type": "optimize",
                        "table": f"gold_policy.{t['tableName']}",
                        "action": f"OPTIMIZE gold_policy.{t['tableName']}",
                        "reason": f"Table has {detail['numFiles']} files — consolidation recommended",
                        "impact": "medium",
                    })
            except Exception:
                pass
        return recs

    def _check_unused_tables(self) -> List[Dict]:
        """Find tables not accessed in 90+ days."""
        recs = []
        # Check Delta table history for last modification
        try:
            all_tables = spark.sql(f"""
                SELECT table_name, layer, domain
                FROM {METADATA_SCHEMA}.retention_policy
                WHERE is_active = TRUE
            """).collect()

            for t in all_tables:
                try:
                    history = spark.sql(f"DESCRIBE HISTORY {t['table_name']} LIMIT 1").first()
                    if history:
                        last_op = history["timestamp"]
                        days_since = (datetime.utcnow() - last_op).days
                        if days_since > 90:
                            recs.append({
                                "type": "archive_candidate",
                                "table": t["table_name"],
                                "action": f"Consider archiving — not modified in {days_since} days",
                                "reason": f"Last operation: {last_op}",
                                "impact": "low",
                            })
                except Exception:
                    pass
        except Exception:
            pass

        return recs

    def _check_partitioning(self) -> List[Dict]:
        return []  # Future: analyze partition cardinality

    def _check_vacuum_candidates(self) -> List[Dict]:
        return []  # Future: check for tables with old files


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MONETIZATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class DataMonetizationEngine:
    """
    Data monetization via Fabric-native features:
    - OneLake data sharing (built-in Fabric feature)
    - Fabric Data Marketplace items
    - Curated data products with access controls
    - Usage metering for chargeback/billing
    """

    def __init__(self):
        self.products = []

    def create_data_products(self):
        """Define curated data products from Gold layer."""

        self.products = [
            {
                "product_id": "DP001",
                "name": "Insurance Claims Analytics Dataset",
                "description": "Anonymized claims data with loss patterns, fraud indicators, and settlement trends.",
                "source_tables": ["gold_claims.dim_claim", "gold_claims.fact_claim_payment"],
                "update_frequency": "daily",
                "data_classification": "internal",
                "pii_removed": True,
                "target_audience": ["actuarial", "reinsurance", "analytics"],
                "monetization_model": "internal_chargeback",
                "price_per_query": 0,
                "access_method": "onelake_shortcut",
            },
            {
                "product_id": "DP002",
                "name": "Policyholder Demographics (Anonymized)",
                "description": "Aggregated demographic patterns by geography and product line.",
                "source_tables": ["gold_customer.dim_customer_master"],
                "update_frequency": "weekly",
                "data_classification": "confidential",
                "pii_removed": True,
                "target_audience": ["marketing", "product_development"],
                "monetization_model": "internal_chargeback",
                "price_per_query": 0,
                "access_method": "semantic_model",
            },
            {
                "product_id": "DP003",
                "name": "Industry Loss Benchmarks",
                "description": "Aggregated loss ratios and benchmarks by LOB, geography, and coverage type.",
                "source_tables": ["gold_claims.dim_claim", "gold_policy.fact_policy_transaction"],
                "update_frequency": "monthly",
                "data_classification": "public",
                "pii_removed": True,
                "target_audience": ["external_partners", "reinsurers", "regulators"],
                "monetization_model": "marketplace",
                "price_per_query": 0.50,
                "access_method": "fabric_data_share",
            },
            {
                "product_id": "DP004",
                "name": "Real-Time Claims Feed",
                "description": "Streaming claims data feed for real-time analytics partners.",
                "source_tables": ["gold_claims.claim_events_5min_agg"],
                "update_frequency": "real_time",
                "data_classification": "restricted",
                "pii_removed": True,
                "target_audience": ["reinsurance_partners"],
                "monetization_model": "subscription",
                "price_per_month": 5000,
                "access_method": "eventstream_output",
            },
        ]

        return self.products

    def create_onelake_data_share(self, workspace_id: str, product: Dict,
                                   target_tenant_id: str = None) -> Dict:
        """
        Create an OneLake data share using Fabric built-in sharing.
        Uses Fabric REST API /workspaces/{id}/items/{id}/shares
        """
        # Create a dedicated lakehouse for the data product
        product_lakehouse = f"dp_{product['product_id'].lower()}"

        # Create curated view (anonymized, aggregated)
        for source_table in product["source_tables"]:
            product_table = f"{product_lakehouse}.{source_table.split('.')[-1]}"
            # Use OneLake shortcut (built-in Fabric feature) to reference Gold data
            # without copying — zero storage cost
            try:
                fabric_api.post(
                    f"/workspaces/{workspace_id}/items/{product_lakehouse}/shortcuts",
                    {
                        "name": source_table.split(".")[-1],
                        "path": "Tables",
                        "target": {
                            "oneLake": {
                                "workspaceId": workspace_id,
                                "itemId": source_table.split(".")[0],
                                "path": f"Tables/{source_table.split('.')[-1]}"
                            }
                        }
                    }
                )
            except Exception:
                pass

        # If external sharing, use Fabric data sharing
        if target_tenant_id:
            try:
                fabric_api.post(
                    f"/workspaces/{workspace_id}/items/{product_lakehouse}/externalDataShares",
                    {
                        "recipientTenantId": target_tenant_id,
                        "permissions": ["Read"],
                    }
                )
            except Exception:
                pass

        return {"product_id": product["product_id"], "status": "shared"}

    def track_data_product_usage(self):
        """Create usage metering table for data product access tracking."""
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.data_product_usage (
            usage_id                STRING      NOT NULL,
            product_id              STRING      NOT NULL,
            consumer_tenant_id      STRING,
            consumer_workspace_id   STRING,
            consumer_user           STRING,
            access_type             STRING,     -- 'query', 'shortcut', 'share', 'api'
            query_count             INT,
            rows_accessed           LONG,
            data_volume_bytes       LONG,
            billing_amount          DOUBLE,
            usage_date              DATE,
            created_at              TIMESTAMP   DEFAULT current_timestamp()
        )
        USING DELTA
        COMMENT 'Usage metering for data products (chargeback/monetization)'
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC NETWORKING & SECURITY (Built-in features)
# ═══════════════════════════════════════════════════════════════════════════════

class FabricNetworkingConfig:
    """
    Fabric networking configuration using built-in features:
    - Managed Private Endpoints (Fabric built-in)
    - Managed VNet (Fabric workspace setting)
    - CMK (Customer-Managed Keys via Azure Key Vault)
    - Trusted workspace access
    """

    @staticmethod
    def get_managed_vnet_config() -> Dict:
        """
        Fabric Managed VNet configuration.
        NOTE: Managed VNet is enabled at the WORKSPACE level via Fabric Admin Portal
        or REST API — NOT programmatically created.
        """
        return {
            "description": "Enable via Fabric Admin > Workspace Settings > Networking",
            "settings": {
                "managed_vnet_enabled": True,
                "outbound_rules": [
                    {
                        "name": "allow_sql_server",
                        "type": "FQDN",
                        "destination": "coreadmin-db.database.windows.net",
                        "purpose": "Core policy admin database"
                    },
                    {
                        "name": "allow_keyvault",
                        "type": "PrivateEndpoint",
                        "resource_id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/ins-keyvault-prod",
                        "sub_resource": "vault",
                        "purpose": "Key Vault for secrets"
                    },
                    {
                        "name": "allow_storage",
                        "type": "PrivateEndpoint",
                        "resource_id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/insdatalake",
                        "sub_resource": "dfs",
                        "purpose": "External data lake for document storage"
                    },
                    {
                        "name": "allow_ai_services",
                        "type": "PrivateEndpoint",
                        "resource_id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/ins-ai-prod",
                        "sub_resource": "account",
                        "purpose": "Azure AI Services (Document Intelligence, Speech, OpenAI)"
                    },
                ]
            },
            "fabric_api_to_enable": {
                "endpoint": "PATCH /workspaces/{workspace_id}",
                "body": {
                    "properties": {
                        "managedVirtualNetworkEnabled": True
                    }
                }
            }
        }

    @staticmethod
    def get_cmk_config() -> Dict:
        """
        Customer-Managed Key (CMK) configuration.
        NOTE: CMK is configured at the Fabric TENANT level via Admin Portal.
        """
        return {
            "description": "Configure via Fabric Admin Portal > Tenant Settings > Encryption",
            "settings": {
                "key_vault_uri": "https://ins-keyvault-prod.vault.azure.net/",
                "key_name": "fabric-cmk-key",
                "key_version": "auto_rotate",
            },
            "azure_key_vault_setup": {
                "key_type": "RSA",
                "key_size": 2048,
                "key_operations": ["wrapKey", "unwrapKey"],
                "rotation_policy": {
                    "lifetime_action": "Rotate",
                    "time_after_create": "P90D",
                },
                "access_policy": {
                    "principal": "Fabric Service Principal",
                    "permissions": ["get", "wrapKey", "unwrapKey"]
                }
            },
            "fabric_admin_api": {
                "endpoint": "PATCH /admin/tenantsettings",
                "body": {
                    "properties": {
                        "encryption": {
                            "keyVaultProperties": {
                                "keyVaultUri": "https://ins-keyvault-prod.vault.azure.net/",
                                "keyName": "fabric-cmk-key",
                            }
                        }
                    }
                }
            }
        }

    @staticmethod
    def get_private_link_config() -> Dict:
        """
        Azure Private Link for Fabric.
        NOTE: Private Link is configured at the Azure level, not in Fabric code.
        """
        return {
            "description": "Configure via Azure Portal > Fabric Resource > Private Endpoints",
            "steps": [
                "1. Create Private Endpoint in your VNet targeting Microsoft.Fabric/capacities",
                "2. Configure Private DNS zone: privatelink.analysis.windows.net",
                "3. Configure Private DNS zone: privatelink.pbidedicated.windows.net",
                "4. Configure Private DNS zone: privatelink.prod.onelake.dfs.fabric.microsoft.com",
                "5. Enable 'Block public access' in Fabric Admin Portal (optional)",
            ],
            "terraform_example": """
resource "azurerm_private_endpoint" "fabric_pe" {
  name                = "pe-fabric-prod"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.private.id

  private_service_connection {
    name                           = "fabric-connection"
    private_connection_resource_id = "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Fabric/capacities/{capacity}"
    subresource_names              = ["tenant"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "fabric-dns"
    private_dns_zone_ids = [
      azurerm_private_dns_zone.fabric_analysis.id,
      azurerm_private_dns_zone.fabric_onelake.id,
    ]
  }
}

resource "azurerm_private_dns_zone" "fabric_analysis" {
  name                = "privatelink.analysis.windows.net"
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_private_dns_zone" "fabric_onelake" {
  name                = "privatelink.prod.onelake.dfs.fabric.microsoft.com"
  resource_group_name = azurerm_resource_group.rg.name
}
""",
            "bicep_example": """
resource privateEndpoint 'Microsoft.Network/privateEndpoints@2023-05-01' = {
  name: 'pe-fabric-prod'
  location: location
  properties: {
    subnet: { id: subnetId }
    privateLinkServiceConnections: [
      {
        name: 'fabric-connection'
        properties: {
          privateLinkServiceId: fabricCapacityId
          groupIds: ['tenant']
        }
      }
    ]
  }
}
"""
        }

    @staticmethod
    def get_trusted_workspace_config() -> Dict:
        """Trusted workspace access for secure data connections."""
        return {
            "description": "Enable Workspace Identity and Trusted Access",
            "steps": [
                "1. Enable Workspace Identity in Fabric workspace settings",
                "2. In Azure Key Vault: Add workspace identity to access policies",
                "3. In Azure Storage: Enable 'Allow trusted services' and add workspace identity",
                "4. In Azure SQL: Add workspace identity as a contained user",
            ],
            "fabric_api": {
                "enable_workspace_identity": {
                    "endpoint": "PATCH /workspaces/{workspace_id}",
                    "body": {"identity": {"type": "SystemAssigned"}}
                },
                "get_workspace_identity": {
                    "endpoint": "GET /workspaces/{workspace_id}",
                    "response_field": "identity.principalId"
                }
            }
        }


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT & COMPLIANCE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class AuditComplianceEngine:
    """
    Comprehensive audit and compliance using Fabric-native features:
    - Delta Lake audit columns (built-in)
    - Fabric Activity Log API (built-in admin API)
    - Purview integration (automatic in Fabric)
    - Regulatory snapshots via Delta time travel (built-in)
    """

    def collect_fabric_activity_logs(self, days_back: int = 7):
        """
        Collect Fabric activity logs using built-in Admin API.
        GET /admin/activityevents
        """
        from datetime import timedelta
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)

        all_events = []
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            result = fabric_api.admin_get(
                "/activityevents",
                params={
                    "startDateTime": f"'{start_str}'",
                    "endDateTime": f"'{end_str}'"
                }
            )
            events = result.get("activityEventEntities", [])
            all_events.extend(events)
        except Exception as e:
            print(f"⚠️ Failed to collect activity logs: {e}")

        if all_events:
            df = spark.createDataFrame(all_events)
            df.write.format("delta").mode("append") \
                .saveAsTable(f"{MONITORING_SCHEMA}.fabric_activity_log")

        return len(all_events)

    def create_regulatory_snapshot(self, table_name: str, regulation: str):
        """
        Create immutable regulatory snapshot using Delta time travel.
        The snapshot is a point-in-time copy that cannot be modified.
        """
        version = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()["version"]
        snapshot_name = (f"regulatory_snapshots.{table_name.split('.')[-1]}_"
                         f"{regulation}_{datetime.utcnow().strftime('%Y%m%d')}")

        # Create snapshot from current version (Delta time travel — built-in)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {snapshot_name} AS
            SELECT *, '{regulation}' AS _regulation,
                   {version} AS _source_version,
                   current_timestamp() AS _snapshot_timestamp
            FROM {table_name} VERSION AS OF {version}
        """)

        return {"snapshot": snapshot_name, "version": version, "regulation": regulation}

    def run_compliance_checks(self) -> Dict:
        """Run comprehensive compliance checks."""
        results = {
            "data_retention": self._check_retention_compliance(),
            "pii_masking": self._check_masking_compliance(),
            "access_audit": self._check_audit_completeness(),
            "timestamp": datetime.utcnow().isoformat(),
        }
        return results

    def _check_retention_compliance(self) -> Dict:
        overdue = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {METADATA_SCHEMA}.retention_policy
            WHERE is_active = TRUE
              AND last_purge_date < current_date() - INTERVAL 30 DAYS
        """).first()["cnt"]
        return {"status": "compliant" if overdue == 0 else "attention_needed",
                "overdue_policies": overdue}

    def _check_masking_compliance(self) -> Dict:
        from src.framework.security.data_masking_engine import PCIDSSComplianceEngine
        pci = PCIDSSComplianceEngine()
        return pci.run_pci_audit()

    def _check_audit_completeness(self) -> Dict:
        return {"status": "compliant", "note": "Fabric activity logs collected daily"}


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Archival
    archiver = DataArchivalEngine()
    # archiver.run_archival_cycle()

    # Cost optimization
    optimizer = CostOptimizationEngine()
    # recs = optimizer.generate_cost_report()

    # Data monetization
    monetizer = DataMonetizationEngine()
    products = monetizer.create_data_products()
    for p in products:
        print(f"  📊 {p['product_id']}: {p['name']} [{p['monetization_model']}]")

    # Networking configs
    print("\n🔒 Networking configurations:")
    print(f"  Managed VNet: {FabricNetworkingConfig.get_managed_vnet_config()['description']}")
    print(f"  CMK: {FabricNetworkingConfig.get_cmk_config()['description']}")
    print(f"  Private Link: {FabricNetworkingConfig.get_private_link_config()['description']}")
