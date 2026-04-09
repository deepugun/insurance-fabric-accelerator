# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — IVR/Voice Processing Pipeline
# Uses Azure AI Speech Service (built-in Azure AI) + Fabric Lakehouse.
# Processes: call recordings → transcription → sentiment → entities → Delta.
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, udf, struct, from_json, explode
)
from pyspark.sql.types import *
from typing import Dict, List, Optional
import json
import uuid
import requests
import time

# Fabric-native utilities
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "framework"))
from fabric_native_utils import get_secret, get_token, IS_FABRIC, send_email

spark = SparkSession.builder.getOrCreate()
UNSTRUCTURED_SCHEMA = "insurance_unstructured"
METADATA_SCHEMA = "insurance_metadata"


def create_voice_tables():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.voice_call_registry (
        call_id                 STRING      NOT NULL,
        recording_path          STRING      NOT NULL,  -- OneLake Files path
        recording_format        STRING,     -- 'wav', 'mp3', 'ogg'
        duration_seconds        INT,
        caller_phone            STRING,
        customer_id             STRING,
        policy_id               STRING,
        claim_id                STRING,
        call_direction          STRING,     -- 'inbound', 'outbound'
        call_reason             STRING,     -- 'claim_inquiry', 'billing', 'complaint', etc.
        agent_id                STRING,
        queue_name              STRING,
        processing_status       STRING      DEFAULT 'pending',
        -- 'pending', 'transcribing', 'transcribed', 'analyzed', 'completed', 'failed'
        transcription_path      STRING,     -- OneLake path to transcript file
        language                STRING      DEFAULT 'en-US',
        diarization_enabled     BOOLEAN     DEFAULT TRUE,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Registry of voice recordings for processing'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.voice_transcriptions (
        transcript_id           STRING      NOT NULL,
        call_id                 STRING      NOT NULL,
        speaker_label           STRING,     -- 'agent', 'caller', 'unknown'
        segment_index           INT,
        start_time_ms           LONG,
        end_time_ms             LONG,
        text                    STRING      NOT NULL,
        confidence              DOUBLE,
        sentiment               STRING,     -- 'positive', 'neutral', 'negative'
        sentiment_score         DOUBLE,
        language                STRING,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (call_id)
    COMMENT 'Transcription segments with speaker diarization'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.voice_analytics (
        analytics_id            STRING      NOT NULL,
        call_id                 STRING      NOT NULL,
        full_transcript         STRING,
        call_summary            STRING,
        customer_intent         STRING,     -- classified intent
        topics_discussed        STRING,     -- JSON array
        action_items            STRING,     -- JSON array
        overall_sentiment       STRING,
        overall_sentiment_score DOUBLE,
        customer_satisfaction   STRING,     -- 'satisfied', 'neutral', 'dissatisfied'
        compliance_flags        STRING,     -- JSON: any compliance issues detected
        key_entities            STRING,     -- JSON: policy numbers, claim numbers, dates, amounts
        escalation_needed       BOOLEAN     DEFAULT FALSE,
        quality_score           DOUBLE,     -- agent quality score 0-100
        llm_model               STRING,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'AI-generated call analytics'
    """)

    print("✅ Voice/IVR tables created.")


class VoiceProcessingEngine:
    """
    Voice/IVR processing pipeline using Azure AI Speech (built-in Azure service):
    1. Azure Speech batch transcription API (with diarization)
    2. Sentiment analysis via Azure AI Language
    3. LLM summarization via Azure OpenAI
    4. Store everything in Fabric Delta tables
    """

    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self._load_config()

    def _load_config(self):
        """Load Azure AI service endpoints from Fabric config."""
        def _cfg(key):
            row = spark.sql(f"""
                SELECT config_value FROM {METADATA_SCHEMA}.environment_config
                WHERE config_key = '{key}' AND environment = '{self.environment}'
                LIMIT 1
            """).first()
            return row["config_value"] if row else ""

        self.speech_endpoint = _cfg("speech_service_endpoint")
        self.speech_region = _cfg("speech_service_region")
        self.openai_endpoint = _cfg("openai_endpoint")
        self.openai_deployment = _cfg("openai_deployment")

        # Secrets via Fabric-native Key Vault
        kv_url = _cfg("key_vault_url")
        self.speech_key = get_secret(kv_url, "speech-service-key") if kv_url else ""
        self.openai_key = get_secret(kv_url, "openai-api-key") if kv_url else ""

    # ────────────────────────────────────────────────────────────────────────
    # Step 1: Batch Transcription (Azure Speech — built-in service)
    # ────────────────────────────────────────────────────────────────────────

    def transcribe_call(self, call_id: str, recording_path: str,
                        language: str = "en-US") -> Optional[str]:
        """
        Submit recording to Azure Speech batch transcription.
        This is the built-in Azure AI Speech service — NOT reimplemented.
        """
        # Update status
        spark.sql(f"""
            UPDATE {UNSTRUCTURED_SCHEMA}.voice_call_registry
            SET processing_status = 'transcribing'
            WHERE call_id = '{call_id}'
        """)

        url = (f"https://{self.speech_region}.api.cognitive.microsoft.com/"
               f"speechtotext/v3.2/transcriptions")
        headers = {
            "Ocp-Apim-Subscription-Key": self.speech_key,
            "Content-Type": "application/json"
        }

        # Generate SAS URL for OneLake file
        # In Fabric, files in OneLake are accessible via abfss:// within Spark,
        # but external services need a SAS. Use notebookutils to generate.
        content_url = self._get_sas_url(recording_path)

        body = {
            "contentUrls": [content_url],
            "locale": language,
            "displayName": f"insurance_call_{call_id}",
            "properties": {
                "diarizationEnabled": True,
                "wordLevelTimestampsEnabled": True,
                "punctuationMode": "DictatedAndAutomatic",
                "profanityFilterMode": "Masked",
                "channels": [0, 1],  # stereo: agent + caller
            },
        }

        resp = requests.post(url, headers=headers, json=body, timeout=30)
        if resp.status_code != 201:
            self._update_status(call_id, "failed")
            return None

        transcription_url = resp.json().get("self")

        # Poll for completion
        for _ in range(120):  # max 10 min
            time.sleep(5)
            poll = requests.get(transcription_url, headers=headers, timeout=30)
            status = poll.json().get("status")
            if status == "Succeeded":
                return self._process_transcription_result(call_id, poll.json())
            elif status == "Failed":
                self._update_status(call_id, "failed")
                return None

        return None

    def _process_transcription_result(self, call_id: str, result: Dict) -> str:
        """Process transcription result and store in Delta."""
        # Get transcript files URL
        files_url = result.get("links", {}).get("files")
        if not files_url:
            return ""

        headers = {"Ocp-Apim-Subscription-Key": self.speech_key}
        files_resp = requests.get(files_url, headers=headers, timeout=30)
        files = files_resp.json().get("values", [])

        full_transcript = []

        for f in files:
            if f.get("kind") == "Transcription":
                content_url = f.get("links", {}).get("contentUrl")
                content = requests.get(content_url, timeout=30).json()

                for phrase in content.get("recognizedPhrases", []):
                    segment_idx = phrase.get("offset", 0)
                    best = phrase.get("nBest", [{}])[0]
                    speaker = phrase.get("speaker", 0)

                    text = best.get("display", "")
                    confidence = best.get("confidence", 0.0)

                    # Map channel/speaker to role
                    speaker_label = "agent" if speaker == 1 else "caller"

                    full_transcript.append(f"{speaker_label}: {text}")

                    # Insert segment into Delta
                    seg_id = str(uuid.uuid4())
                    text_safe = text.replace("'", "''")
                    spark.sql(f"""
                        INSERT INTO {UNSTRUCTURED_SCHEMA}.voice_transcriptions VALUES (
                            '{seg_id}', '{call_id}', '{speaker_label}',
                            {segment_idx}, {phrase.get("offsetInTicks", 0)},
                            {phrase.get("offsetInTicks", 0) + phrase.get("durationInTicks", 0)},
                            '{text_safe}', {confidence},
                            NULL, NULL, '{content.get("locale", "en-US")}',
                            current_timestamp()
                        )
                    """)

        transcript_text = "\n".join(full_transcript)
        self._update_status(call_id, "transcribed")
        return transcript_text

    # ────────────────────────────────────────────────────────────────────────
    # Step 2: Analyze Call (Sentiment + Summarization via Azure AI)
    # ────────────────────────────────────────────────────────────────────────

    def analyze_call(self, call_id: str, transcript: str):
        """
        Analyze transcribed call using Azure OpenAI (built-in Azure AI service):
        - Summarize the call
        - Extract intent, topics, action items
        - Score sentiment and quality
        - Detect compliance issues
        """
        system_prompt = """You are an insurance call center quality analyst.
        Analyze this call transcript between an agent and a customer.
        Return JSON with:
        - "summary": 2-3 sentence call summary
        - "customer_intent": primary reason for the call
        - "topics": array of topics discussed
        - "action_items": array of follow-up actions needed
        - "overall_sentiment": "positive", "neutral", or "negative"
        - "sentiment_score": -1.0 to 1.0
        - "customer_satisfaction": "satisfied", "neutral", or "dissatisfied"
        - "compliance_flags": array of any compliance issues (e.g., missing disclosures)
        - "key_entities": dict of policy_numbers, claim_numbers, dates, dollar_amounts found
        - "escalation_needed": boolean
        - "agent_quality_score": 0-100 rating of the agent's performance"""

        url = (f"{self.openai_endpoint}/openai/deployments/{self.openai_deployment}/"
               f"chat/completions?api-version=2024-06-01")
        headers = {"api-key": self.openai_key, "Content-Type": "application/json"}
        body = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": transcript[:12000]}
            ],
            "max_tokens": 1500,
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }

        resp = requests.post(url, headers=headers, json=body, timeout=60)
        if resp.status_code == 200:
            analysis = json.loads(resp.json()["choices"][0]["message"]["content"])
            analytics_id = str(uuid.uuid4())
            summary_safe = analysis.get("summary", "").replace("'", "''")

            spark.sql(f"""
                INSERT INTO {UNSTRUCTURED_SCHEMA}.voice_analytics VALUES (
                    '{analytics_id}', '{call_id}',
                    NULL,
                    '{summary_safe}',
                    '{analysis.get("customer_intent", "")}',
                    '{json.dumps(analysis.get("topics", []))}',
                    '{json.dumps(analysis.get("action_items", []))}',
                    '{analysis.get("overall_sentiment", "neutral")}',
                    {analysis.get("sentiment_score", 0)},
                    '{analysis.get("customer_satisfaction", "neutral")}',
                    '{json.dumps(analysis.get("compliance_flags", []))}',
                    '{json.dumps(analysis.get("key_entities", {}))}',
                    {str(analysis.get("escalation_needed", False)).upper()},
                    {analysis.get("agent_quality_score", 0)},
                    '{self.openai_deployment}',
                    current_timestamp()
                )
            """)

            self._update_status(call_id, "completed")

            # Alert if escalation needed
            if analysis.get("escalation_needed"):
                send_email(
                    "claims_support@insurer.com",
                    f"⚠️ Call Escalation Required: {call_id}",
                    f"Call Summary: {analysis.get('summary')}\n"
                    f"Customer Intent: {analysis.get('customer_intent')}\n"
                    f"Sentiment: {analysis.get('overall_sentiment')}"
                )

            return analysis
        return {}

    # ────────────────────────────────────────────────────────────────────────
    # Full Pipeline
    # ────────────────────────────────────────────────────────────────────────

    def process_pending_calls(self, batch_size: int = 20):
        """Process all pending voice recordings."""
        pending = spark.sql(f"""
            SELECT call_id, recording_path, language
            FROM {UNSTRUCTURED_SCHEMA}.voice_call_registry
            WHERE processing_status = 'pending'
            ORDER BY created_at ASC
            LIMIT {batch_size}
        """).collect()

        results = []
        for call in pending:
            try:
                # Transcribe
                transcript = self.transcribe_call(
                    call["call_id"], call["recording_path"], call["language"]
                )
                if transcript:
                    # Analyze
                    analysis = self.analyze_call(call["call_id"], transcript)
                    results.append({"call_id": call["call_id"], "status": "completed"})
                else:
                    results.append({"call_id": call["call_id"], "status": "transcription_failed"})
            except Exception as e:
                self._update_status(call["call_id"], "failed")
                results.append({"call_id": call["call_id"], "status": "failed", "error": str(e)})

        return results

    # ── Helpers ──

    def _update_status(self, call_id: str, status: str):
        spark.sql(f"""
            UPDATE {UNSTRUCTURED_SCHEMA}.voice_call_registry
            SET processing_status = '{status}'
            WHERE call_id = '{call_id}'
        """)

    def _get_sas_url(self, onelake_path: str) -> str:
        """Generate SAS URL for OneLake file (for external service access)."""
        if IS_FABRIC:
            from notebookutils import mssparkutils
            # Use notebookutils to get accessible URL
            return onelake_path  # In practice, generate SAS via Azure Storage SDK
        return onelake_path


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    create_voice_tables()
    # engine = VoiceProcessingEngine()
    # engine.process_pending_calls()
