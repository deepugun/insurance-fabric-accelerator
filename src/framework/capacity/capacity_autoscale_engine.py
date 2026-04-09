# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Fabric Capacity Auto-Scale Engine
# Uses Fabric REST API /capacities endpoints (built-in).
# Scales capacity up/down based on shift schedules, usage patterns,
# and developer activity — optimized for cost in lower environments.
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import uuid

# Use Fabric-native utilities (never reimplement)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from fabric_native_utils import fabric_api, get_secret, send_email, IS_FABRIC

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"
MONITORING_SCHEMA = "insurance_monitoring"


# ═══════════════════════════════════════════════════════════════════════════════
# CAPACITY METADATA TABLES
# ═══════════════════════════════════════════════════════════════════════════════

def create_capacity_tables():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.capacity_config (
        config_id               STRING      NOT NULL,
        capacity_id             STRING      NOT NULL,   -- Fabric capacity GUID
        capacity_name           STRING      NOT NULL,
        environment             STRING      NOT NULL,   -- 'dev', 'test', 'prod'
        region                  STRING,
        -- SKU scaling tiers (Fabric built-in SKUs)
        min_sku                 STRING      NOT NULL,   -- 'F2', 'F4', etc.
        max_sku                 STRING      NOT NULL,   -- 'F64', 'F128', etc.
        default_sku             STRING      NOT NULL,
        current_sku             STRING,
        -- Schedule-based scaling
        scale_up_cron           STRING,                 -- e.g., '0 7 * * 1-5' (7am Mon-Fri)
        scale_down_cron         STRING,                 -- e.g., '0 19 * * 1-5' (7pm Mon-Fri)
        weekend_sku             STRING,                 -- lower SKU for weekends
        -- Usage-based scaling
        cpu_scale_up_pct        INT         DEFAULT 80, -- scale up when CPU > 80%
        cpu_scale_down_pct      INT         DEFAULT 20, -- scale down when CPU < 20%
        usage_lookback_min      INT         DEFAULT 30,
        -- Cost controls
        monthly_budget_usd      DOUBLE,
        current_month_spend_usd DOUBLE      DEFAULT 0,
        auto_pause_enabled      BOOLEAN     DEFAULT FALSE,  -- Fabric auto-pause
        pause_after_idle_min    INT         DEFAULT 30,
        -- Permissions
        auto_scale_enabled      BOOLEAN     DEFAULT TRUE,
        require_approval        BOOLEAN     DEFAULT FALSE,  -- for prod
        approval_email          STRING,
        notification_email      STRING,
        is_active               BOOLEAN     DEFAULT TRUE,
        created_at              TIMESTAMP   DEFAULT current_timestamp(),
        updated_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Capacity scaling configuration per environment'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.capacity_usage_log (
        log_id                  STRING      NOT NULL,
        capacity_id             STRING      NOT NULL,
        environment             STRING,
        timestamp               TIMESTAMP   NOT NULL,
        sku                     STRING,
        cu_usage_pct            DOUBLE,     -- Capacity Unit utilization %
        cu_throttling_pct       DOUBLE,     -- % of requests throttled
        active_spark_sessions   INT,
        active_notebooks        INT,
        active_pipelines        INT,
        active_refreshes        INT,
        total_cu_seconds        LONG,       -- total CU-seconds consumed
        overload_detected       BOOLEAN     DEFAULT FALSE,
        smoothed_usage_pct      DOUBLE,     -- rolling average
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (environment)
    COMMENT 'Capacity utilization metrics log'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.capacity_scale_events (
        event_id                STRING      NOT NULL,
        capacity_id             STRING      NOT NULL,
        environment             STRING,
        event_type              STRING      NOT NULL,
        -- 'scale_up', 'scale_down', 'pause', 'resume',
        -- 'budget_alert', 'throttle_alert'
        from_sku                STRING,
        to_sku                  STRING,
        trigger                 STRING,     -- 'schedule', 'usage', 'manual', 'budget'
        reason                  STRING,
        initiated_by            STRING,     -- 'agent', 'schedule', 'user:{email}'
        status                  STRING,     -- 'pending', 'approved', 'executed', 'failed', 'rejected'
        approved_by             STRING,
        error_message           STRING,
        cost_impact_hourly_usd  DOUBLE,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Audit trail of all capacity scaling events'
    """)

    print("✅ Capacity management tables created.")


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC SKU DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

FABRIC_SKUS = {
    # SKU: (CU count, hourly_cost_usd_estimate)
    "F2":    (2,    0.36),
    "F4":    (4,    0.72),
    "F8":    (8,    1.44),
    "F16":   (16,   2.88),
    "F32":   (32,   5.76),
    "F64":   (64,   11.52),
    "F128":  (128,  23.04),
    "F256":  (256,  46.08),
    "F512":  (512,  92.16),
    "F1024": (1024, 184.32),
    "F2048": (2048, 368.64),
}

SKU_ORDER = list(FABRIC_SKUS.keys())


# ═══════════════════════════════════════════════════════════════════════════════
# CAPACITY AUTO-SCALE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class FabricCapacityManager:
    """
    Manages Fabric capacity scaling using built-in REST APIs:
    - GET  /capacities/{id}                 → read current SKU
    - PATCH /capacities/{id}                → change SKU (scale)
    - POST /capacities/{id}/suspend         → pause capacity
    - POST /capacities/{id}/resume          → resume capacity
    - GET  /admin/capacities/{id}/metrics   → usage metrics (Admin API)
    """

    def __init__(self, environment: str = "prod"):
        self.environment = environment

    # ────────────────────────────────────────────────────────────────────────
    # Read Current State (Fabric REST API — built-in)
    # ────────────────────────────────────────────────────────────────────────

    def get_capacity_info(self, capacity_id: str) -> Dict:
        """Get capacity details via Fabric REST API (built-in)."""
        return fabric_api.get(f"/capacities/{capacity_id}")

    def get_capacity_metrics(self, capacity_id: str) -> Dict:
        """
        Get capacity utilization metrics via Fabric Admin API.
        Uses built-in /admin/capacities endpoint.
        """
        # Fabric Admin API for capacity metrics
        return fabric_api.admin_get(
            f"/capacities/{capacity_id}/metrics",
            params={"timespan": "PT1H"}  # last 1 hour
        )

    def get_workload_status(self, capacity_id: str) -> Dict:
        """Get active workloads on the capacity."""
        try:
            # Use Fabric Admin API to get workload summary
            workspaces = fabric_api.admin_get(
                f"/capacities/{capacity_id}/workspaces"
            )
            return {
                "workspace_count": len(workspaces.get("value", [])),
                "capacity_id": capacity_id,
            }
        except Exception:
            return {"workspace_count": 0, "capacity_id": capacity_id}

    # ────────────────────────────────────────────────────────────────────────
    # Scale Operations (Fabric REST API — built-in)
    # ────────────────────────────────────────────────────────────────────────

    def scale_capacity(self, capacity_id: str, target_sku: str, reason: str,
                       trigger: str = "agent") -> Dict:
        """
        Scale Fabric capacity to a new SKU.
        Uses PATCH /capacities/{id} — Fabric built-in scaling.
        """
        config = self._get_config(capacity_id)
        current_sku = self._get_current_sku(capacity_id)

        # Validate against bounds
        min_idx = SKU_ORDER.index(config["min_sku"])
        max_idx = SKU_ORDER.index(config["max_sku"])
        target_idx = SKU_ORDER.index(target_sku)

        if target_idx < min_idx:
            target_sku = config["min_sku"]
        elif target_idx > max_idx:
            target_sku = config["max_sku"]

        if target_sku == current_sku:
            return {"status": "no_change", "sku": current_sku}

        # Check if approval needed (prod)
        if config.get("require_approval") and trigger == "agent":
            self._request_approval(config, current_sku, target_sku, reason)
            return {"status": "pending_approval", "from": current_sku, "to": target_sku}

        # Execute scale via Fabric REST API (built-in)
        try:
            fabric_api.patch(
                f"/capacities/{capacity_id}",
                {"sku": {"name": target_sku, "tier": "Fabric"}}
            )

            # Log the event
            self._log_scale_event(
                capacity_id, "scale_up" if target_idx > SKU_ORDER.index(current_sku) else "scale_down",
                current_sku, target_sku, trigger, reason, "executed"
            )

            # Notify
            if config.get("notification_email"):
                event_type = "⬆️ SCALED UP" if target_idx > SKU_ORDER.index(current_sku) else "⬇️ SCALED DOWN"
                send_email(
                    config["notification_email"],
                    f"[Fabric Capacity] {event_type}: {config['capacity_name']}",
                    f"Capacity: {config['capacity_name']}\n"
                    f"Environment: {self.environment}\n"
                    f"From: {current_sku} → To: {target_sku}\n"
                    f"Reason: {reason}\n"
                    f"Trigger: {trigger}\n"
                    f"Estimated hourly cost: ${FABRIC_SKUS[target_sku][1]:.2f}/hr"
                )

            return {"status": "executed", "from": current_sku, "to": target_sku}

        except Exception as e:
            self._log_scale_event(
                capacity_id, "scale_up", current_sku, target_sku,
                trigger, reason, "failed", error=str(e)
            )
            return {"status": "failed", "error": str(e)}

    def suspend_capacity(self, capacity_id: str, reason: str = "cost_optimization"):
        """Suspend (pause) Fabric capacity — built-in feature."""
        try:
            fabric_api.post(f"/capacities/{capacity_id}/suspend")
            self._log_scale_event(capacity_id, "pause", None, None,
                                  "agent", reason, "executed")
            return {"status": "suspended"}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def resume_capacity(self, capacity_id: str, reason: str = "scheduled"):
        """Resume Fabric capacity — built-in feature."""
        try:
            fabric_api.post(f"/capacities/{capacity_id}/resume")
            self._log_scale_event(capacity_id, "resume", None, None,
                                  "agent", reason, "executed")
            return {"status": "resumed"}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    # ────────────────────────────────────────────────────────────────────────
    # Auto-Scale Logic (runs on schedule)
    # ────────────────────────────────────────────────────────────────────────

    def evaluate_and_scale(self, capacity_id: str):
        """
        Evaluate current usage and scale if needed.
        Uses Fabric built-in capacity metrics API.
        """
        config = self._get_config(capacity_id)
        if not config or not config.get("auto_scale_enabled"):
            return {"action": "disabled"}

        current_sku = self._get_current_sku(capacity_id)
        current_idx = SKU_ORDER.index(current_sku)

        # 1. Check shift-based scaling
        shift_decision = self._evaluate_shift_schedule(config)
        if shift_decision:
            return self.scale_capacity(capacity_id, shift_decision["target_sku"],
                                       shift_decision["reason"], "schedule")

        # 2. Check usage-based scaling
        usage = self._get_smoothed_usage(capacity_id, config.get("usage_lookback_min", 30))

        if usage is not None:
            # Scale UP if usage exceeds threshold
            if usage > config.get("cpu_scale_up_pct", 80):
                next_sku = SKU_ORDER[min(current_idx + 1, len(SKU_ORDER) - 1)]
                return self.scale_capacity(
                    capacity_id, next_sku,
                    f"Usage {usage:.0f}% > threshold {config['cpu_scale_up_pct']}%",
                    "usage"
                )

            # Scale DOWN if usage below threshold
            if usage < config.get("cpu_scale_down_pct", 20):
                prev_sku = SKU_ORDER[max(current_idx - 1, 0)]
                return self.scale_capacity(
                    capacity_id, prev_sku,
                    f"Usage {usage:.0f}% < threshold {config['cpu_scale_down_pct']}%",
                    "usage"
                )

        # 3. Check budget
        budget_action = self._check_budget(config, current_sku)
        if budget_action:
            return budget_action

        return {"action": "no_change", "sku": current_sku, "usage_pct": usage}

    def _evaluate_shift_schedule(self, config: Dict) -> Optional[Dict]:
        """Check if shift schedule requires scaling."""
        now = datetime.utcnow()
        day_of_week = now.weekday()  # 0=Mon, 6=Sun
        hour = now.hour

        # Weekend scaling
        if day_of_week >= 5 and config.get("weekend_sku"):
            current = config.get("current_sku", config["default_sku"])
            if current != config["weekend_sku"]:
                return {
                    "target_sku": config["weekend_sku"],
                    "reason": f"Weekend auto-scale (day={day_of_week})"
                }

        # Check team shift schedule from monitoring tables
        active_shifts = spark.sql(f"""
            SELECT COUNT(*) AS active_count
            FROM {MONITORING_SCHEMA}.team_shift_schedule
            WHERE shift_start <= current_timestamp()
              AND shift_end >= current_timestamp()
              AND is_active = TRUE
        """).first()

        if active_shifts and active_shifts["active_count"] == 0:
            # No one on shift — scale to minimum
            if config.get("environment") in ("dev", "test"):
                return {
                    "target_sku": config["min_sku"],
                    "reason": "No active team members on shift — scaling to minimum"
                }

        return None

    def _check_budget(self, config: Dict, current_sku: str) -> Optional[Dict]:
        """Check monthly budget and scale down or alert if approaching limit."""
        if not config.get("monthly_budget_usd"):
            return None

        spent = config.get("current_month_spend_usd", 0)
        budget = config["monthly_budget_usd"]
        day = datetime.utcnow().day
        days_in_month = 30  # approximation
        projected = (spent / max(day, 1)) * days_in_month

        if projected > budget * 1.1:
            # Over budget projection — scale down
            current_idx = SKU_ORDER.index(current_sku)
            target_sku = SKU_ORDER[max(current_idx - 1, 0)]
            self._log_scale_event(
                config["capacity_id"], "budget_alert", current_sku, target_sku,
                "budget", f"Projected ${projected:.0f} > budget ${budget:.0f}", "executed"
            )
            if config.get("notification_email"):
                send_email(
                    config["notification_email"],
                    f"⚠️ Budget Alert: {config['capacity_name']}",
                    f"Projected spend: ${projected:.0f}\n"
                    f"Monthly budget: ${budget:.0f}\n"
                    f"Action: Scaling down to {target_sku}"
                )
            return {"action": "budget_scale_down", "target_sku": target_sku}

        return None

    # ────────────────────────────────────────────────────────────────────────
    # Dev/Test Environment Optimization
    # ────────────────────────────────────────────────────────────────────────

    def optimize_dev_test_costs(self):
        """
        Special optimization for dev/test environments:
        - Pause when no activity
        - Scale to minimum during off-hours
        - Auto-suspend idle capacities
        Uses Fabric built-in suspend/resume.
        """
        configs = spark.sql(f"""
            SELECT * FROM {MONITORING_SCHEMA}.capacity_config
            WHERE environment IN ('dev', 'test')
              AND is_active = TRUE
              AND auto_scale_enabled = TRUE
        """).collect()

        for cfg in configs:
            config = cfg.asDict()

            # Check for any active Spark sessions
            try:
                workload = self.get_workload_status(config["capacity_id"])
                # If no active workspaces or sessions, consider pausing
                if config.get("auto_pause_enabled"):
                    last_activity = spark.sql(f"""
                        SELECT MAX(timestamp) AS last_ts
                        FROM {MONITORING_SCHEMA}.capacity_usage_log
                        WHERE capacity_id = '{config["capacity_id"]}'
                          AND (active_spark_sessions > 0 OR active_notebooks > 0
                               OR active_pipelines > 0)
                    """).first()

                    if last_activity and last_activity["last_ts"]:
                        idle_min = (datetime.utcnow() - last_activity["last_ts"]).total_seconds() / 60
                        if idle_min > config.get("pause_after_idle_min", 30):
                            self.suspend_capacity(
                                config["capacity_id"],
                                f"Dev/Test idle for {idle_min:.0f} minutes"
                            )
                            continue

            except Exception:
                pass

            # Run normal evaluation
            self.evaluate_and_scale(config["capacity_id"])

    # ────────────────────────────────────────────────────────────────────────
    # Usage Collection (polls Fabric metrics API)
    # ────────────────────────────────────────────────────────────────────────

    def collect_usage_metrics(self, capacity_id: str):
        """Collect and store capacity usage metrics from Fabric Admin API."""
        try:
            # Fabric built-in metrics endpoint
            info = self.get_capacity_info(capacity_id)
            metrics = self.get_capacity_metrics(capacity_id)

            log_id = str(uuid.uuid4())
            cu_pct = metrics.get("capacityUtilizationPercentage", 0)
            throttle_pct = metrics.get("throttlingPercentage", 0)

            spark.sql(f"""
                INSERT INTO {MONITORING_SCHEMA}.capacity_usage_log VALUES (
                    '{log_id}', '{capacity_id}', '{self.environment}',
                    current_timestamp(),
                    '{info.get("sku", {}).get("name", "unknown")}',
                    {cu_pct}, {throttle_pct},
                    {metrics.get("activeSparkSessions", 0)},
                    {metrics.get("activeNotebooks", 0)},
                    {metrics.get("activePipelines", 0)},
                    {metrics.get("activeRefreshes", 0)},
                    {metrics.get("totalCUSeconds", 0)},
                    {str(throttle_pct > 10).upper()},
                    NULL, current_timestamp()
                )
            """)

            # Update smoothed usage
            self._update_smoothed_usage(capacity_id, cu_pct)

        except Exception as e:
            print(f"⚠️ Failed to collect metrics for {capacity_id}: {e}")

    # ────────────────────────────────────────────────────────────────────────
    # Helpers
    # ────────────────────────────────────────────────────────────────────────

    def _get_config(self, capacity_id: str) -> Optional[Dict]:
        row = spark.sql(f"""
            SELECT * FROM {MONITORING_SCHEMA}.capacity_config
            WHERE capacity_id = '{capacity_id}' AND is_active = TRUE
            LIMIT 1
        """).first()
        return row.asDict() if row else None

    def _get_current_sku(self, capacity_id: str) -> str:
        info = self.get_capacity_info(capacity_id)
        return info.get("sku", {}).get("name", "F2")

    def _get_smoothed_usage(self, capacity_id: str, lookback_min: int) -> Optional[float]:
        row = spark.sql(f"""
            SELECT AVG(cu_usage_pct) AS avg_usage
            FROM {MONITORING_SCHEMA}.capacity_usage_log
            WHERE capacity_id = '{capacity_id}'
              AND timestamp >= current_timestamp() - INTERVAL {lookback_min} MINUTES
        """).first()
        return row["avg_usage"] if row and row["avg_usage"] is not None else None

    def _update_smoothed_usage(self, capacity_id: str, current_pct: float):
        # Exponential moving average
        prev = self._get_smoothed_usage(capacity_id, 60)
        if prev is not None:
            smoothed = prev * 0.7 + current_pct * 0.3
        else:
            smoothed = current_pct

        spark.sql(f"""
            UPDATE {MONITORING_SCHEMA}.capacity_usage_log
            SET smoothed_usage_pct = {smoothed}
            WHERE capacity_id = '{capacity_id}'
              AND log_id = (
                  SELECT log_id FROM {MONITORING_SCHEMA}.capacity_usage_log
                  WHERE capacity_id = '{capacity_id}'
                  ORDER BY timestamp DESC LIMIT 1
              )
        """)

    def _log_scale_event(self, capacity_id: str, event_type: str,
                         from_sku: str, to_sku: str, trigger: str,
                         reason: str, status: str, error: str = None):
        event_id = str(uuid.uuid4())
        cost_impact = (FABRIC_SKUS.get(to_sku, (0, 0))[1] -
                       FABRIC_SKUS.get(from_sku, (0, 0))[1]) if from_sku and to_sku else 0

        reason_safe = (reason or "").replace("'", "''")
        error_safe = (error or "").replace("'", "''")
        spark.sql(f"""
            INSERT INTO {MONITORING_SCHEMA}.capacity_scale_events VALUES (
                '{event_id}', '{capacity_id}', '{self.environment}',
                '{event_type}', '{from_sku or ""}', '{to_sku or ""}',
                '{trigger}', '{reason_safe}', 'agent', '{status}',
                NULL, '{error_safe}', {cost_impact}, current_timestamp()
            )
        """)

    def _request_approval(self, config: Dict, from_sku: str, to_sku: str, reason: str):
        if config.get("approval_email"):
            send_email(
                config["approval_email"],
                f"🔐 Approval Required: Scale {config['capacity_name']}",
                f"Scale request for: {config['capacity_name']}\n"
                f"From: {from_sku} → To: {to_sku}\n"
                f"Reason: {reason}\n\n"
                f"Reply APPROVE or REJECT."
            )


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    create_capacity_tables()

    manager = FabricCapacityManager(environment="dev")

    # Collect metrics
    # manager.collect_usage_metrics("capacity-guid-here")

    # Auto-scale evaluation
    # manager.evaluate_and_scale("capacity-guid-here")

    # Dev/test cost optimization
    # manager.optimize_dev_test_costs()
