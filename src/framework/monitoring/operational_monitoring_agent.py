# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Operational Monitoring Agent
# Agent-based monitoring for EVERY pipeline, activity, report, refresh,
# dataflow, notebook execution, and capacity event across the platform.
# Designed for a 10-person DataOps team at Chubb/Allstate scale.
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, expr, when, datediff, hour,
    minute, count, avg, max as spark_max, min as spark_min,
    sum as spark_sum, window, from_json, to_json, struct,
    date_format, concat, round as spark_round
)
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import uuid
import requests
from typing import Optional, Dict, List, Any

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"
MONITORING_SCHEMA = "insurance_monitoring"


# ═══════════════════════════════════════════════════════════════════════════════
# MONITORING METADATA TABLES
# ═══════════════════════════════════════════════════════════════════════════════

def create_monitoring_tables():
    """Create all monitoring-specific metadata tables."""

    # ── Monitored Asset Registry ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.monitored_assets (
        asset_id                STRING      NOT NULL,
        asset_name              STRING      NOT NULL,
        asset_type              STRING      NOT NULL,
        -- 'pipeline', 'notebook', 'dataflow', 'semantic_model', 'report',
        -- 'lakehouse', 'eventhouse', 'kql_queryset', 'spark_job', 'reflex'
        workspace_id            STRING      NOT NULL,
        workspace_name          STRING,
        fabric_item_id          STRING,
        domain                  STRING,
        owner_team              STRING,
        owner_email             STRING,
        criticality             STRING      DEFAULT 'medium',
        -- 'critical', 'high', 'medium', 'low'
        sla_max_duration_min    INT,
        sla_max_failure_rate    DOUBLE      DEFAULT 0.05,
        sla_freshness_min       INT,
        monitoring_enabled      BOOLEAN     DEFAULT TRUE,
        alert_on_failure        BOOLEAN     DEFAULT TRUE,
        alert_on_sla_breach     BOOLEAN     DEFAULT TRUE,
        alert_on_anomaly        BOOLEAN     DEFAULT TRUE,
        auto_retry_enabled      BOOLEAN     DEFAULT FALSE,
        max_auto_retries        INT         DEFAULT 2,
        escalation_chain        STRING,
        -- JSON: [{"level":1,"team":"dataops","minutes":15},
        --        {"level":2,"team":"manager","minutes":60}]
        tags                    STRING,     -- JSON tags for grouping
        is_active               BOOLEAN     DEFAULT TRUE,
        created_at              TIMESTAMP   DEFAULT current_timestamp(),
        updated_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Registry of all Fabric assets under monitoring'
    """)

    # ── Activity Execution Log (unified across all asset types) ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.activity_execution_log (
        log_id                  STRING      NOT NULL,
        asset_id                STRING      NOT NULL,
        asset_type              STRING      NOT NULL,
        activity_name           STRING,
        run_id                  STRING,
        parent_run_id           STRING,
        status                  STRING      NOT NULL,
        -- 'queued', 'in_progress', 'succeeded', 'failed',
        -- 'cancelled', 'timed_out', 'skipped'
        start_time              TIMESTAMP,
        end_time                TIMESTAMP,
        duration_seconds        INT,
        records_read            LONG,
        records_written         LONG,
        data_volume_bytes       LONG,
        cpu_time_ms             LONG,
        spark_executor_count    INT,
        error_code              STRING,
        error_message           STRING,
        error_category          STRING,
        -- 'transient', 'config', 'data', 'capacity', 'auth', 'unknown'
        retry_attempt           INT         DEFAULT 0,
        triggered_by            STRING,
        -- 'schedule', 'manual', 'api', 'event', 'retry', 'agent'
        input_parameters        STRING,     -- JSON
        output_metrics          STRING,     -- JSON
        environment             STRING      DEFAULT 'prod',
        correlation_id          STRING,     -- trace across pipelines
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (asset_type, status)
    COMMENT 'Unified execution log for all monitored Fabric activities'
    """)

    # ── Report / Semantic Model Refresh Log ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.refresh_execution_log (
        refresh_id              STRING      NOT NULL,
        asset_id                STRING      NOT NULL,
        asset_type              STRING      NOT NULL,
        -- 'semantic_model', 'report', 'dataflow_gen2', 'datamart'
        refresh_type            STRING,     -- 'scheduled', 'on_demand', 'api'
        status                  STRING      NOT NULL,
        start_time              TIMESTAMP,
        end_time                TIMESTAMP,
        duration_seconds        INT,
        objects_refreshed       INT,
        rows_transferred        LONG,
        error_message           STRING,
        service_exception_json  STRING,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Tracks all report and semantic model refreshes'
    """)

    # ── Agent Decision Log ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.agent_decision_log (
        decision_id             STRING      NOT NULL,
        agent_type              STRING      NOT NULL,
        -- 'ops_monitor', 'capacity_scaler', 'incident_responder',
        -- 'cost_optimizer', 'security_auditor'
        trigger_event           STRING      NOT NULL,
        trigger_source          STRING,
        analysis_summary        STRING,
        decision_made           STRING      NOT NULL,
        -- 'alert', 'retry', 'scale_up', 'scale_down', 'escalate',
        -- 'archive', 'quarantine', 'no_action', 'investigate'
        action_taken            STRING,
        action_parameters       STRING,     -- JSON
        confidence_score        DOUBLE,
        affected_assets         STRING,     -- JSON array of asset_ids
        human_override          BOOLEAN     DEFAULT FALSE,
        override_by             STRING,
        override_reason         STRING,
        execution_status        STRING,     -- 'pending', 'executed', 'failed'
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'All decisions made by operational AI agents with full audit trail'
    """)

    # ── SLA Tracking ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.sla_tracking (
        sla_id                  STRING      NOT NULL,
        asset_id                STRING      NOT NULL,
        sla_type                STRING      NOT NULL,
        -- 'freshness', 'availability', 'duration', 'quality'
        measurement_time        TIMESTAMP   NOT NULL,
        sla_target              DOUBLE      NOT NULL,
        sla_actual              DOUBLE      NOT NULL,
        sla_met                 BOOLEAN     NOT NULL,
        breach_duration_min     INT,
        domain                  STRING,
        period                  STRING,     -- 'hourly', 'daily', 'weekly', 'monthly'
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (sla_type)
    COMMENT 'SLA compliance tracking across all monitored assets'
    """)

    # ── Incident Management ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.incidents (
        incident_id             STRING      NOT NULL,
        title                   STRING      NOT NULL,
        severity                STRING      NOT NULL,
        -- 'P1_critical', 'P2_high', 'P3_medium', 'P4_low'
        status                  STRING      DEFAULT 'open',
        -- 'open', 'acknowledged', 'investigating', 'mitigating',
        -- 'resolved', 'closed', 'false_positive'
        category                STRING,
        -- 'pipeline_failure', 'data_quality', 'capacity',
        -- 'security', 'performance', 'data_loss'
        affected_assets         STRING,     -- JSON array
        affected_domains        STRING,     -- JSON array
        root_cause              STRING,
        resolution              STRING,
        detected_by             STRING,     -- 'agent', 'alert', 'manual', 'user'
        detected_at             TIMESTAMP   NOT NULL,
        acknowledged_at         TIMESTAMP,
        acknowledged_by         STRING,
        resolved_at             TIMESTAMP,
        resolved_by             STRING,
        time_to_detect_min      INT,
        time_to_resolve_min     INT,
        escalation_level        INT         DEFAULT 0,
        related_incidents       STRING,     -- JSON array of incident_ids
        postmortem_link         STRING,
        tags                    STRING,
        created_at              TIMESTAMP   DEFAULT current_timestamp(),
        updated_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Incident management for the DataOps team'
    """)

    # ── Team Shift Schedule ──
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.team_shift_schedule (
        shift_id                STRING      NOT NULL,
        team_member             STRING      NOT NULL,
        email                   STRING      NOT NULL,
        role                    STRING      NOT NULL,
        -- 'dataops_engineer', 'data_engineer', 'team_lead',
        -- 'on_call_primary', 'on_call_secondary', 'manager'
        shift_start             TIMESTAMP   NOT NULL,
        shift_end               TIMESTAMP   NOT NULL,
        timezone                STRING      DEFAULT 'America/New_York',
        is_on_call              BOOLEAN     DEFAULT FALSE,
        is_active               BOOLEAN     DEFAULT TRUE,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Team shift schedule for routing alerts and capacity scaling'
    """)

    print("✅ All monitoring tables created.")


# ═══════════════════════════════════════════════════════════════════════════════
# OPERATIONAL MONITORING AGENT
# ═══════════════════════════════════════════════════════════════════════════════

class OperationalMonitoringAgent:
    """
    AI-powered operational monitoring agent that:
    - Polls ALL Fabric items via REST APIs
    - Detects failures, anomalies, SLA breaches
    - Auto-retries transient failures
    - Escalates based on shift schedule
    - Logs every decision for audit
    - Feeds Central Cockpit metrics
    """

    FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

    def __init__(self, workspace_id: str, environment: str = "prod"):
        self.workspace_id = workspace_id
        self.environment = environment
        self.agent_run_id = str(uuid.uuid4())
        self.decisions = []

    # ────────────────────────────────────────────────────────────────────────
    # API Client (Fabric REST API)
    # ────────────────────────────────────────────────────────────────────────

    def _get_fabric_token(self) -> str:
        """Get access token via Managed Identity (Fabric notebook) or SP."""
        # In Fabric notebook:
        # from notebookutils import mssparkutils
        # return mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
        raise NotImplementedError("Implement via notebookutils in Fabric environment")

    def _api_get(self, path: str) -> dict:
        """Make authenticated GET to Fabric REST API."""
        token = self._get_fabric_token()
        resp = requests.get(
            f"{self.FABRIC_API_BASE}{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()

    def _api_post(self, path: str, body: dict = None) -> dict:
        """Make authenticated POST to Fabric REST API."""
        token = self._get_fabric_token()
        resp = requests.post(
            f"{self.FABRIC_API_BASE}{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json=body or {},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    # ────────────────────────────────────────────────────────────────────────
    # Discovery: Scan All Fabric Items
    # ────────────────────────────────────────────────────────────────────────

    def discover_workspace_items(self) -> List[Dict]:
        """Discover all items in the workspace for monitoring."""
        items = []
        item_types = [
            "Lakehouse", "Notebook", "Pipeline", "SemanticModel",
            "Report", "Dataflow", "SparkJobDefinition", "Eventstream",
            "KQLQueryset", "Reflex", "MLModel", "MLExperiment"
        ]

        for item_type in item_types:
            try:
                resp = self._api_get(
                    f"/workspaces/{self.workspace_id}/items?type={item_type}"
                )
                for item in resp.get("value", []):
                    items.append({
                        "fabric_item_id": item["id"],
                        "asset_name": item["displayName"],
                        "asset_type": item["type"],
                        "description": item.get("description", ""),
                    })
            except Exception as e:
                self._log_decision(
                    "ops_monitor", f"discovery_error_{item_type}",
                    f"Failed to discover {item_type}: {str(e)}",
                    "alert", confidence=1.0
                )
        return items

    def register_assets(self, items: List[Dict]):
        """Auto-register discovered items into monitored_assets."""
        for item in items:
            existing = spark.sql(f"""
                SELECT 1 FROM {MONITORING_SCHEMA}.monitored_assets
                WHERE fabric_item_id = '{item["fabric_item_id"]}'
            """).count()

            if existing == 0:
                criticality = self._infer_criticality(item)
                spark.sql(f"""
                    INSERT INTO {MONITORING_SCHEMA}.monitored_assets
                    VALUES (
                        '{str(uuid.uuid4())}', '{item["asset_name"]}',
                        '{item["asset_type"]}', '{self.workspace_id}', NULL,
                        '{item["fabric_item_id"]}', NULL, 'dataops',
                        NULL, '{criticality}', NULL, 0.05, NULL,
                        TRUE, TRUE, TRUE, TRUE,
                        {str(criticality == 'critical').upper()}, 2,
                        NULL, NULL, TRUE, current_timestamp(), current_timestamp()
                    )
                """)

    def _infer_criticality(self, item: Dict) -> str:
        """Infer criticality based on asset type and naming conventions."""
        name = item["asset_name"].lower()
        asset_type = item["asset_type"].lower()

        if any(kw in name for kw in ["prod", "gold", "finance", "regulatory", "claims"]):
            return "critical"
        if any(kw in name for kw in ["silver", "billing", "customer"]):
            return "high"
        if asset_type in ("pipeline", "semanticmodel"):
            return "high"
        return "medium"

    # ────────────────────────────────────────────────────────────────────────
    # Pipeline Monitoring
    # ────────────────────────────────────────────────────────────────────────

    def monitor_pipeline_runs(self, lookback_hours: int = 4):
        """
        Poll all pipeline runs and log status.
        Detect failures, long-running, stuck pipelines.
        """
        assets = spark.sql(f"""
            SELECT * FROM {MONITORING_SCHEMA}.monitored_assets
            WHERE asset_type = 'Pipeline' AND monitoring_enabled = TRUE
        """).collect()

        for asset in assets:
            try:
                # Get recent runs via Fabric API
                runs = self._api_get(
                    f"/workspaces/{self.workspace_id}/items/"
                    f"{asset['fabric_item_id']}/jobs/instances"
                    f"?startDateTime={self._lookback_iso(lookback_hours)}"
                )

                for run in runs.get("value", []):
                    self._process_pipeline_run(asset, run)

            except Exception as e:
                self._log_decision(
                    "ops_monitor",
                    f"pipeline_poll_error:{asset['asset_name']}",
                    f"Error polling pipeline: {str(e)}",
                    "alert", confidence=1.0
                )

    def _process_pipeline_run(self, asset, run: Dict):
        """Process a single pipeline run — detect issues and act."""
        status = run.get("status", "Unknown")
        run_id = run.get("id", "")
        start_time = run.get("startTimeUtc")
        end_time = run.get("endTimeUtc")

        # Log execution
        self._log_activity(
            asset_id=asset["asset_id"],
            asset_type="Pipeline",
            activity_name=asset["asset_name"],
            run_id=run_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            error_message=run.get("failureReason"),
        )

        # ── Failure handling ──
        if status == "Failed":
            error_category = self._classify_error(run.get("failureReason", ""))

            if error_category == "transient" and asset["auto_retry_enabled"]:
                retry_count = self._get_retry_count(run_id)
                if retry_count < asset["max_auto_retries"]:
                    self._retry_pipeline(asset, run_id, retry_count + 1)
                    return

            # Create incident
            self._create_incident(
                title=f"Pipeline Failed: {asset['asset_name']}",
                severity=self._severity_from_criticality(asset["criticality"]),
                category="pipeline_failure",
                affected_assets=[asset["asset_id"]],
                root_cause=run.get("failureReason"),
                detected_by="agent"
            )

            # Route alert based on shift schedule
            self._route_alert(asset, f"Pipeline '{asset['asset_name']}' failed: {run.get('failureReason')}")

        # ── SLA breach detection ──
        if status == "InProgress" and asset.get("sla_max_duration_min"):
            duration_min = self._calc_duration_minutes(start_time)
            if duration_min > asset["sla_max_duration_min"]:
                self._log_decision(
                    "ops_monitor",
                    f"sla_breach:{asset['asset_name']}",
                    f"Pipeline running {duration_min}min > SLA {asset['sla_max_duration_min']}min",
                    "escalate", confidence=0.95
                )
                self._route_alert(
                    asset,
                    f"⚠️ SLA BREACH: Pipeline '{asset['asset_name']}' running "
                    f"{duration_min} min (SLA: {asset['sla_max_duration_min']} min)"
                )

    # ────────────────────────────────────────────────────────────────────────
    # Semantic Model / Report Refresh Monitoring
    # ────────────────────────────────────────────────────────────────────────

    def monitor_refresh_operations(self, lookback_hours: int = 24):
        """Monitor all semantic model and report refreshes."""
        for asset_type in ["SemanticModel", "Report"]:
            assets = spark.sql(f"""
                SELECT * FROM {MONITORING_SCHEMA}.monitored_assets
                WHERE asset_type = '{asset_type}' AND monitoring_enabled = TRUE
            """).collect()

            for asset in assets:
                try:
                    # Get refresh history
                    refreshes = self._api_get(
                        f"/workspaces/{self.workspace_id}/semanticModels/"
                        f"{asset['fabric_item_id']}/refreshes"
                    )

                    for refresh in refreshes.get("value", []):
                        status = refresh.get("status", "Unknown")
                        self._log_refresh(asset, refresh)

                        if status == "Failed":
                            self._create_incident(
                                title=f"Refresh Failed: {asset['asset_name']}",
                                severity="P2_high" if asset["criticality"] in ("critical", "high") else "P3_medium",
                                category="pipeline_failure",
                                affected_assets=[asset["asset_id"]],
                                root_cause=refresh.get("serviceExceptionJson"),
                                detected_by="agent"
                            )
                            self._route_alert(
                                asset,
                                f"❌ Refresh failed for {asset_type} '{asset['asset_name']}'"
                            )

                except Exception as e:
                    pass  # Non-critical — logged in next cycle

    # ────────────────────────────────────────────────────────────────────────
    # Notebook / Spark Job Monitoring
    # ────────────────────────────────────────────────────────────────────────

    def monitor_spark_sessions(self):
        """Monitor active Spark sessions for stuck/long-running jobs."""
        try:
            sessions = self._api_get(
                f"/workspaces/{self.workspace_id}/sparkCompute/sessions"
            )
            for session in sessions.get("value", []):
                state = session.get("state", "")
                duration_min = self._calc_duration_minutes(session.get("startTime"))

                # Detect zombie sessions (running > 4 hours with no activity)
                if state == "idle" and duration_min > 240:
                    self._log_decision(
                        "ops_monitor",
                        f"zombie_session:{session.get('id')}",
                        f"Spark session idle for {duration_min} min — wasting capacity",
                        "investigate",
                        confidence=0.85,
                        action_params={"session_id": session.get("id")}
                    )

                # Detect stuck sessions
                if state == "busy" and duration_min > 360:
                    self._log_decision(
                        "ops_monitor",
                        f"stuck_session:{session.get('id')}",
                        f"Spark session busy for {duration_min} min — possible hang",
                        "alert",
                        confidence=0.90
                    )

        except Exception:
            pass

    # ────────────────────────────────────────────────────────────────────────
    # Anomaly Detection (statistical)
    # ────────────────────────────────────────────────────────────────────────

    def detect_execution_anomalies(self):
        """
        Compare recent execution metrics against historical baselines.
        Flag anomalies in duration, record counts, failure rates.
        """
        # Duration anomalies
        anomalies = spark.sql(f"""
            WITH recent AS (
                SELECT asset_id, asset_type, duration_seconds,
                       records_written, status
                FROM {MONITORING_SCHEMA}.activity_execution_log
                WHERE start_time >= current_timestamp() - INTERVAL 4 HOURS
                  AND status = 'succeeded'
            ),
            baseline AS (
                SELECT asset_id,
                       AVG(duration_seconds) AS avg_duration,
                       STDDEV(duration_seconds) AS std_duration,
                       AVG(records_written) AS avg_records,
                       STDDEV(records_written) AS std_records
                FROM {MONITORING_SCHEMA}.activity_execution_log
                WHERE start_time >= current_timestamp() - INTERVAL 30 DAYS
                  AND status = 'succeeded'
                GROUP BY asset_id
            )
            SELECT r.asset_id, r.asset_type, r.duration_seconds,
                   b.avg_duration, b.std_duration,
                   r.records_written, b.avg_records, b.std_records,
                   CASE
                     WHEN r.duration_seconds > b.avg_duration + 3 * b.std_duration
                       THEN 'duration_spike'
                     WHEN r.records_written < b.avg_records - 3 * b.std_records
                       THEN 'low_volume'
                     WHEN r.records_written > b.avg_records + 3 * b.std_records
                       THEN 'high_volume'
                   END AS anomaly_type
            FROM recent r
            JOIN baseline b ON r.asset_id = b.asset_id
            WHERE r.duration_seconds > b.avg_duration + 3 * COALESCE(b.std_duration, 0)
               OR r.records_written < b.avg_records - 3 * COALESCE(b.std_records, 0)
               OR r.records_written > b.avg_records + 3 * COALESCE(b.std_records, 0)
        """).collect()

        for anomaly in anomalies:
            self._log_decision(
                "ops_monitor",
                f"anomaly:{anomaly['anomaly_type']}:{anomaly['asset_id']}",
                f"Anomaly detected: {anomaly['anomaly_type']} for asset {anomaly['asset_id']}. "
                f"Current: duration={anomaly['duration_seconds']}s, "
                f"records={anomaly['records_written']}. "
                f"Baseline: avg_duration={anomaly['avg_duration']:.0f}s, "
                f"avg_records={anomaly['avg_records']:.0f}",
                "investigate",
                confidence=0.80
            )

    # ── Failure rate monitoring ──
    def check_failure_rates(self):
        """Check rolling failure rates per asset against SLA thresholds."""
        failure_rates = spark.sql(f"""
            SELECT
                a.asset_id, a.asset_name, a.sla_max_failure_rate,
                COUNT(*) AS total_runs,
                SUM(CASE WHEN e.status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
                SUM(CASE WHEN e.status = 'failed' THEN 1 ELSE 0 END) * 1.0
                    / COUNT(*) AS failure_rate
            FROM {MONITORING_SCHEMA}.monitored_assets a
            JOIN {MONITORING_SCHEMA}.activity_execution_log e
              ON a.asset_id = e.asset_id
            WHERE e.start_time >= current_timestamp() - INTERVAL 24 HOURS
              AND a.monitoring_enabled = TRUE
            GROUP BY a.asset_id, a.asset_name, a.sla_max_failure_rate
            HAVING failure_rate > a.sla_max_failure_rate
        """).collect()

        for fr in failure_rates:
            self._create_incident(
                title=f"High failure rate: {fr['asset_name']} ({fr['failure_rate']:.1%})",
                severity="P2_high",
                category="pipeline_failure",
                affected_assets=[fr["asset_id"]],
                root_cause=f"Failure rate {fr['failure_rate']:.1%} exceeds SLA {fr['sla_max_failure_rate']:.1%}",
                detected_by="agent"
            )

    # ────────────────────────────────────────────────────────────────────────
    # Health Score Computation (for Central Cockpit)
    # ────────────────────────────────────────────────────────────────────────

    def compute_domain_health_scores(self) -> DataFrame:
        """
        Compute health score (0-100) per domain combining:
        - Pipeline success rate (40%)
        - Data quality score (30%)
        - SLA compliance (20%)
        - Freshness (10%)
        """
        return spark.sql(f"""
            WITH pipeline_health AS (
                SELECT
                    a.domain,
                    SUM(CASE WHEN e.status = 'succeeded' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(COUNT(*), 0) AS pipeline_success_pct
                FROM {MONITORING_SCHEMA}.monitored_assets a
                JOIN {MONITORING_SCHEMA}.activity_execution_log e
                  ON a.asset_id = e.asset_id
                WHERE e.start_time >= current_timestamp() - INTERVAL 24 HOURS
                GROUP BY a.domain
            ),
            dq_health AS (
                SELECT
                    domain,
                    AVG(pass_percentage) AS dq_score
                FROM {METADATA_SCHEMA}.dq_execution_log dq
                JOIN {METADATA_SCHEMA}.data_quality_rules r
                  ON dq.dq_rule_id = r.dq_rule_id
                WHERE dq.execution_start >= current_timestamp() - INTERVAL 24 HOURS
                GROUP BY domain
            ),
            sla_health AS (
                SELECT
                    a.domain,
                    SUM(CASE WHEN s.sla_met THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(COUNT(*), 0) AS sla_compliance_pct
                FROM {MONITORING_SCHEMA}.sla_tracking s
                JOIN {MONITORING_SCHEMA}.monitored_assets a
                  ON s.asset_id = a.asset_id
                WHERE s.measurement_time >= current_timestamp() - INTERVAL 24 HOURS
                GROUP BY a.domain
            )
            SELECT
                COALESCE(p.domain, d.domain, s.domain) AS domain,
                ROUND(
                    COALESCE(p.pipeline_success_pct, 100) * 0.4 +
                    COALESCE(d.dq_score, 100) * 0.3 +
                    COALESCE(s.sla_compliance_pct, 100) * 0.2 +
                    100 * 0.1,  -- freshness placeholder
                    1
                ) AS health_score,
                COALESCE(p.pipeline_success_pct, 100) AS pipeline_pct,
                COALESCE(d.dq_score, 100) AS dq_pct,
                COALESCE(s.sla_compliance_pct, 100) AS sla_pct,
                current_timestamp() AS computed_at
            FROM pipeline_health p
            FULL OUTER JOIN dq_health d ON p.domain = d.domain
            FULL OUTER JOIN sla_health s ON COALESCE(p.domain, d.domain) = s.domain
        """)

    # ────────────────────────────────────────────────────────────────────────
    # Alert Routing (shift-aware)
    # ────────────────────────────────────────────────────────────────────────

    def _route_alert(self, asset, message: str):
        """Route alert to on-call team member based on current shift."""
        on_call = spark.sql(f"""
            SELECT team_member, email, role
            FROM {MONITORING_SCHEMA}.team_shift_schedule
            WHERE is_on_call = TRUE
              AND shift_start <= current_timestamp()
              AND shift_end >= current_timestamp()
              AND is_active = TRUE
            ORDER BY
                CASE role
                    WHEN 'on_call_primary' THEN 1
                    WHEN 'on_call_secondary' THEN 2
                    WHEN 'team_lead' THEN 3
                    ELSE 4
                END
            LIMIT 1
        """).first()

        if on_call:
            self._send_notification(
                channel="teams",
                recipient=on_call["email"],
                subject=f"[{asset['criticality'].upper()}] {asset['asset_name']}",
                body=message
            )
        else:
            # Fallback: send to default ops channel
            self._send_notification(
                channel="email",
                recipient=self._get_config("notification_email"),
                subject=f"[{asset['criticality'].upper()}] {asset['asset_name']}",
                body=message
            )

    def _send_notification(self, channel: str, recipient: str, subject: str, body: str):
        """Send notification via configured channel."""
        if channel == "teams":
            webhook_url = self._get_config("teams_webhook_url")
            if webhook_url:
                payload = {
                    "type": "message",
                    "attachments": [{
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.4",
                            "body": [
                                {"type": "TextBlock", "text": subject, "weight": "Bolder", "size": "Medium"},
                                {"type": "TextBlock", "text": body, "wrap": True}
                            ]
                        }
                    }]
                }
                try:
                    requests.post(webhook_url, json=payload, timeout=30)
                except Exception:
                    pass

        elif channel == "email":
            # In production: use Logic App, Power Automate, or SendGrid
            # For Fabric: use notebookutils.notification.sendMail()
            pass

    # ────────────────────────────────────────────────────────────────────────
    # Helpers
    # ────────────────────────────────────────────────────────────────────────

    def _classify_error(self, error_msg: str) -> str:
        """Classify error as transient or permanent."""
        if not error_msg:
            return "unknown"
        transient_patterns = [
            "timeout", "throttl", "429", "503", "connection reset",
            "temporary", "retry", "capacity", "busy"
        ]
        error_lower = error_msg.lower()
        if any(p in error_lower for p in transient_patterns):
            return "transient"
        if any(p in error_lower for p in ["auth", "401", "403", "permission"]):
            return "auth"
        if any(p in error_lower for p in ["schema", "column", "type mismatch", "parse"]):
            return "data"
        if any(p in error_lower for p in ["not found", "missing", "config"]):
            return "config"
        return "unknown"

    def _severity_from_criticality(self, criticality: str) -> str:
        mapping = {"critical": "P1_critical", "high": "P2_high", "medium": "P3_medium", "low": "P4_low"}
        return mapping.get(criticality, "P3_medium")

    def _retry_pipeline(self, asset, run_id: str, attempt: int):
        """Auto-retry a failed pipeline."""
        try:
            self._api_post(
                f"/workspaces/{self.workspace_id}/items/"
                f"{asset['fabric_item_id']}/jobs/instances?jobType=Pipeline"
            )
            self._log_decision(
                "ops_monitor",
                f"auto_retry:{asset['asset_name']}",
                f"Auto-retrying pipeline (attempt {attempt})",
                "retry",
                confidence=0.90,
                action_params={"attempt": attempt, "run_id": run_id}
            )
        except Exception as e:
            self._log_decision(
                "ops_monitor",
                f"retry_failed:{asset['asset_name']}",
                f"Auto-retry failed: {str(e)}",
                "escalate",
                confidence=1.0
            )

    def _get_retry_count(self, run_id: str) -> int:
        row = spark.sql(f"""
            SELECT MAX(retry_attempt) AS max_retry
            FROM {MONITORING_SCHEMA}.activity_execution_log
            WHERE run_id = '{run_id}'
        """).first()
        return row["max_retry"] or 0 if row else 0

    def _calc_duration_minutes(self, start_time_str) -> int:
        if not start_time_str:
            return 0
        try:
            if isinstance(start_time_str, str):
                start = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            else:
                start = start_time_str
            return int((datetime.utcnow() - start.replace(tzinfo=None)).total_seconds() / 60)
        except Exception:
            return 0

    def _lookback_iso(self, hours: int) -> str:
        return (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_config(self, key: str) -> str:
        row = spark.sql(f"""
            SELECT config_value FROM {METADATA_SCHEMA}.environment_config
            WHERE config_key = '{key}' AND environment = '{self.environment}' AND is_active = TRUE
            LIMIT 1
        """).first()
        return row["config_value"] if row else None

    # ── Logging helpers ──

    def _log_activity(self, **kwargs):
        """Log an activity execution."""
        log_id = str(uuid.uuid4())
        spark.sql(f"""
            INSERT INTO {MONITORING_SCHEMA}.activity_execution_log VALUES (
                '{log_id}', '{kwargs.get("asset_id")}', '{kwargs.get("asset_type")}',
                '{kwargs.get("activity_name", "")}', '{kwargs.get("run_id", "")}',
                NULL, '{kwargs.get("status")}',
                CAST('{kwargs.get("start_time")}' AS TIMESTAMP),
                CAST('{kwargs.get("end_time")}' AS TIMESTAMP),
                NULL, NULL, NULL, NULL, NULL, NULL,
                '{kwargs.get("error_code", "")}',
                '{kwargs.get("error_message", "")}',
                NULL, 0, 'agent', NULL, NULL, '{self.environment}',
                '{self.agent_run_id}', current_timestamp()
            )
        """)

    def _log_refresh(self, asset, refresh: Dict):
        spark.sql(f"""
            INSERT INTO {MONITORING_SCHEMA}.refresh_execution_log VALUES (
                '{str(uuid.uuid4())}', '{asset["asset_id"]}',
                '{asset["asset_type"]}', '{refresh.get("refreshType", "")}',
                '{refresh.get("status", "")}',
                CAST('{refresh.get("startTime")}' AS TIMESTAMP),
                CAST('{refresh.get("endTime")}' AS TIMESTAMP),
                NULL, NULL, NULL, NULL, NULL, current_timestamp()
            )
        """)

    def _log_decision(self, agent_type: str, trigger: str, analysis: str,
                      decision: str, confidence: float = 0.5,
                      action_params: Dict = None):
        """Log agent decision with full audit trail."""
        decision_id = str(uuid.uuid4())
        params_json = json.dumps(action_params) if action_params else "{}"
        self.decisions.append({
            "decision_id": decision_id,
            "decision": decision,
            "trigger": trigger,
        })
        spark.sql(f"""
            INSERT INTO {MONITORING_SCHEMA}.agent_decision_log VALUES (
                '{decision_id}', '{agent_type}', '{trigger}',
                'monitoring_agent', '{analysis}', '{decision}',
                NULL, '{params_json}', {confidence}, NULL,
                FALSE, NULL, NULL, 'pending', current_timestamp()
            )
        """)

    def _create_incident(self, title: str, severity: str, category: str,
                         affected_assets: List[str], root_cause: str = None,
                         detected_by: str = "agent"):
        """Create a new incident."""
        incident_id = str(uuid.uuid4())
        assets_json = json.dumps(affected_assets)
        root_cause_safe = (root_cause or "").replace("'", "''")
        spark.sql(f"""
            INSERT INTO {MONITORING_SCHEMA}.incidents VALUES (
                '{incident_id}', '{title}', '{severity}', 'open',
                '{category}', '{assets_json}', NULL,
                '{root_cause_safe}', NULL, '{detected_by}',
                current_timestamp(), NULL, NULL, NULL, NULL,
                NULL, NULL, 0, NULL, NULL, NULL,
                current_timestamp(), current_timestamp()
            )
        """)

    # ────────────────────────────────────────────────────────────────────────
    # Main Run Loop
    # ────────────────────────────────────────────────────────────────────────

    def run_full_monitoring_cycle(self):
        """
        Execute a complete monitoring cycle. Designed to run every 5-15 minutes.
        """
        print(f"🔍 Starting monitoring cycle: {self.agent_run_id}")

        # 1. Discover & register new assets
        items = self.discover_workspace_items()
        self.register_assets(items)
        print(f"   📋 Monitoring {len(items)} Fabric items")

        # 2. Monitor pipeline runs
        self.monitor_pipeline_runs(lookback_hours=4)
        print("   ✅ Pipeline runs checked")

        # 3. Monitor refreshes
        self.monitor_refresh_operations(lookback_hours=24)
        print("   ✅ Refresh operations checked")

        # 4. Monitor Spark sessions
        self.monitor_spark_sessions()
        print("   ✅ Spark sessions checked")

        # 5. Detect anomalies
        self.detect_execution_anomalies()
        print("   ✅ Anomaly detection complete")

        # 6. Check failure rates
        self.check_failure_rates()
        print("   ✅ Failure rates checked")

        # 7. Compute health scores
        health_df = self.compute_domain_health_scores()
        health_df.write.mode("overwrite").saveAsTable(
            f"{MONITORING_SCHEMA}.domain_health_scores"
        )
        print("   ✅ Health scores updated")

        print(f"🏁 Monitoring cycle complete. Decisions: {len(self.decisions)}")
        return self.decisions


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    create_monitoring_tables()

    # Get workspace ID from config
    ws_id = spark.sql(f"""
        SELECT config_value FROM {METADATA_SCHEMA}.environment_config
        WHERE config_key = 'fabric_workspace_id' AND environment = 'prod'
    """).first()["config_value"]

    agent = OperationalMonitoringAgent(workspace_id=ws_id, environment="prod")
    decisions = agent.run_full_monitoring_cycle()
