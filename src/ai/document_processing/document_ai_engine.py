# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Unstructured Data & Document AI Pipeline
# Processes: Claim PDFs, medical records, policy documents, correspondence,
#            images (accident photos, ID scans), handwritten forms.
# Uses: Azure AI Document Intelligence, Azure OpenAI, Fabric OneLake.
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, udf, explode, from_json, struct,
    sha2, concat_ws, when, array, to_json, input_file_name, regexp_extract
)
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, List, Optional
import json
import uuid
import requests
import base64

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"
UNSTRUCTURED_SCHEMA = "insurance_unstructured"


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT PROCESSING METADATA TABLES
# ═══════════════════════════════════════════════════════════════════════════════

def create_unstructured_tables():
    """Create tables for unstructured data pipeline tracking."""

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.document_registry (
        document_id             STRING      NOT NULL,
        document_type           STRING      NOT NULL,
        -- 'claim_form', 'medical_record', 'police_report', 'invoice',
        -- 'policy_document', 'correspondence', 'id_scan', 'accident_photo',
        -- 'proof_of_loss', 'appraisal', 'adjuster_report', 'death_certificate',
        -- 'beneficiary_form', 'application_form', 'endorsement_letter'
        source_system           STRING,
        source_path             STRING      NOT NULL,
        onelake_path            STRING,         -- path in OneLake Files section
        file_name               STRING      NOT NULL,
        file_extension          STRING,         -- 'pdf', 'png', 'jpg', 'tiff', 'docx'
        file_size_bytes         LONG,
        mime_type               STRING,
        domain                  STRING      NOT NULL,
        related_entity_type     STRING,         -- 'claim', 'policy', 'customer'
        related_entity_id       STRING,         -- claim_id, policy_id, etc.
        processing_status       STRING      DEFAULT 'pending',
        -- 'pending', 'processing', 'completed', 'failed', 'review_required'
        ocr_completed           BOOLEAN     DEFAULT FALSE,
        ai_extraction_completed BOOLEAN     DEFAULT FALSE,
        classification_label    STRING,         -- AI-assigned document class
        classification_confidence DOUBLE,
        language_detected       STRING,
        page_count              INT,
        has_handwriting         BOOLEAN,
        has_signatures          BOOLEAN,
        has_tables              BOOLEAN,
        has_images              BOOLEAN,
        content_hash            STRING,         -- SHA-256 for dedup
        retention_category      STRING,         -- 'regulatory', 'business', 'temporary'
        retention_expiry_date   DATE,
        pii_detected            BOOLEAN     DEFAULT FALSE,
        pii_elements            STRING,         -- JSON: list of PII types found
        uploaded_by             STRING,
        uploaded_at             TIMESTAMP,
        processed_at            TIMESTAMP,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (document_type, processing_status)
    COMMENT 'Registry of all unstructured documents ingested into the platform'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.document_extracted_data (
        extraction_id           STRING      NOT NULL,
        document_id             STRING      NOT NULL,
        extraction_model        STRING      NOT NULL,
        -- 'document_intelligence', 'custom_model', 'gpt4_vision', 'layout'
        model_version           STRING,
        field_name              STRING      NOT NULL,
        field_value             STRING,
        field_type              STRING,         -- 'string', 'date', 'number', 'currency', 'address'
        confidence              DOUBLE,
        bounding_box            STRING,         -- JSON: page/polygon coordinates
        page_number             INT,
        is_handwritten          BOOLEAN     DEFAULT FALSE,
        validation_status       STRING      DEFAULT 'auto_accepted',
        -- 'auto_accepted', 'low_confidence', 'human_verified', 'corrected'
        corrected_value         STRING,
        corrected_by            STRING,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (extraction_model)
    COMMENT 'Extracted structured data from unstructured documents'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.document_classification_model (
        model_id                STRING      NOT NULL,
        model_name              STRING      NOT NULL,
        model_type              STRING,         -- 'prebuilt', 'custom', 'composed'
        document_intelligence_model_id STRING,  -- Azure DI model ID
        supported_document_types STRING,        -- JSON array
        training_date           TIMESTAMP,
        accuracy                DOUBLE,
        field_definitions       STRING,         -- JSON: expected fields per doc type
        is_active               BOOLEAN     DEFAULT TRUE,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Document AI model registry'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.document_embeddings (
        embedding_id            STRING      NOT NULL,
        document_id             STRING      NOT NULL,
        chunk_index             INT         NOT NULL,
        chunk_text              STRING      NOT NULL,
        embedding_vector        ARRAY<FLOAT>,
        embedding_model         STRING      DEFAULT 'text-embedding-ada-002',
        token_count             INT,
        page_number             INT,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Document embeddings for semantic search and RAG'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNSTRUCTURED_SCHEMA}.document_summaries (
        summary_id              STRING      NOT NULL,
        document_id             STRING      NOT NULL,
        summary_type            STRING,         -- 'executive', 'detailed', 'key_findings'
        summary_text            STRING      NOT NULL,
        key_entities            STRING,         -- JSON: extracted entities
        sentiment               STRING,         -- 'positive', 'neutral', 'negative'
        action_items            STRING,         -- JSON: recommended actions
        llm_model               STRING,
        token_count             INT,
        created_at              TIMESTAMP   DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'AI-generated document summaries'
    """)

    print("✅ Unstructured data tables created.")


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT AI PROCESSING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class DocumentAIEngine:
    """
    End-to-end document processing pipeline:
    1. Ingest documents from OneLake Files
    2. Classify document type (AI)
    3. Extract structured data (Document Intelligence)
    4. Generate embeddings for RAG
    5. Summarize with LLM
    6. PII detection and redaction
    7. Store results in Delta tables
    """

    # Document Intelligence prebuilt models for insurance
    DI_MODELS = {
        "claim_form":       "prebuilt-invoice",         # custom model in prod
        "medical_record":   "prebuilt-healthInsuranceClaim.us",
        "invoice":          "prebuilt-invoice",
        "id_scan":          "prebuilt-idDocument",
        "receipt":          "prebuilt-receipt",
        "policy_document":  "prebuilt-document",
        "correspondence":   "prebuilt-document",
        "accident_photo":   "prebuilt-document",        # + GPT-4 Vision
        "death_certificate":"prebuilt-document",
        "general":          "prebuilt-layout",
    }

    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.di_endpoint = self._get_config("ai_services_endpoint")
        self.di_key = self._get_secret("kv-di-api-key")
        self.openai_endpoint = self._get_config("openai_endpoint")
        self.openai_key = self._get_secret("kv-openai-api-key")
        self.openai_deployment = self._get_config("openai_deployment")

    # ────────────────────────────────────────────────────────────────────────
    # 1. Document Ingestion (OneLake Files)
    # ────────────────────────────────────────────────────────────────────────

    def ingest_documents_from_onelake(self, source_path: str, domain: str,
                                       related_entity_type: str = None):
        """
        Scan OneLake Files for new documents and register them.
        Source path: abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/
        """
        files_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true") \
            .load(source_path)

        files_df = files_df.withColumn("file_name",
            regexp_extract(col("path"), r"[^/]+$", 0)
        ).withColumn("file_ext",
            regexp_extract(col("path"), r"\.([^.]+)$", 1)
        )

        registered = 0
        for row in files_df.collect():
            doc_id = str(uuid.uuid4())
            content_hash = row["path"]  # In prod: hash of content bytes

            # Check for duplicates
            existing = spark.sql(f"""
                SELECT 1 FROM {UNSTRUCTURED_SCHEMA}.document_registry
                WHERE source_path = '{row["path"]}'
            """).count()

            if existing == 0:
                spark.sql(f"""
                    INSERT INTO {UNSTRUCTURED_SCHEMA}.document_registry VALUES (
                        '{doc_id}', 'pending_classification', NULL,
                        '{row["path"]}', '{row["path"]}',
                        '{row["path"].split("/")[-1]}',
                        '{row["path"].split(".")[-1] if "." in row["path"] else "unknown"}',
                        {row["length"]}, NULL, '{domain}',
                        '{related_entity_type or ""}', NULL,
                        'pending', FALSE, FALSE, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL,
                        '{domain}_retention', NULL, FALSE, NULL,
                        NULL, current_timestamp(), NULL, current_timestamp()
                    )
                """)
                registered += 1

        print(f"📄 Registered {registered} new documents from {source_path}")
        return registered

    # ────────────────────────────────────────────────────────────────────────
    # 2. Document Classification
    # ────────────────────────────────────────────────────────────────────────

    def classify_document(self, document_id: str, file_bytes: bytes) -> Dict:
        """
        Classify document type using Azure AI Document Intelligence
        custom classification model, or GPT-4 for complex cases.
        """
        # Step 1: Try Document Intelligence custom classifier
        classification = self._di_classify(file_bytes)

        if classification["confidence"] < 0.7:
            # Step 2: Fallback to GPT-4 Vision for low-confidence cases
            classification = self._gpt4_classify(file_bytes)

        # Update registry
        spark.sql(f"""
            UPDATE {UNSTRUCTURED_SCHEMA}.document_registry
            SET document_type = '{classification["document_type"]}',
                classification_label = '{classification["document_type"]}',
                classification_confidence = {classification["confidence"]},
                processing_status = 'classified'
            WHERE document_id = '{document_id}'
        """)

        return classification

    def _di_classify(self, file_bytes: bytes) -> Dict:
        """Classify using Document Intelligence custom classifier."""
        url = f"{self.di_endpoint}/documentintelligence/documentClassifiers/insurance-classifier:analyze?api-version=2024-11-30"
        headers = {
            "Ocp-Apim-Subscription-Key": self.di_key,
            "Content-Type": "application/octet-stream",
        }
        resp = requests.post(url, headers=headers, data=file_bytes, timeout=120)
        if resp.status_code == 202:
            # Poll for result
            result_url = resp.headers.get("Operation-Location")
            import time
            for _ in range(30):
                time.sleep(2)
                result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": self.di_key})
                if result.json().get("status") == "succeeded":
                    docs = result.json().get("analyzeResult", {}).get("documents", [])
                    if docs:
                        return {
                            "document_type": docs[0]["docType"],
                            "confidence": docs[0]["confidence"],
                        }
            return {"document_type": "unknown", "confidence": 0.0}
        return {"document_type": "unknown", "confidence": 0.0}

    def _gpt4_classify(self, file_bytes: bytes) -> Dict:
        """Classify using GPT-4 Vision for complex/mixed documents."""
        b64_content = base64.b64encode(file_bytes).decode("utf-8")
        url = f"{self.openai_endpoint}/openai/deployments/{self.openai_deployment}/chat/completions?api-version=2024-06-01"
        headers = {"api-key": self.openai_key, "Content-Type": "application/json"}
        body = {
            "messages": [
                {"role": "system", "content": """You are an insurance document classifier.
                Classify the document into one of: claim_form, medical_record, police_report,
                invoice, policy_document, correspondence, id_scan, accident_photo,
                proof_of_loss, appraisal, adjuster_report, death_certificate,
                beneficiary_form, application_form, endorsement_letter.
                Return JSON: {"document_type": "...", "confidence": 0.0-1.0, "reason": "..."}"""},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:application/pdf;base64,{b64_content[:50000]}"}}
                ]}
            ],
            "max_tokens": 200,
            "response_format": {"type": "json_object"}
        }
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        if resp.status_code == 200:
            content = json.loads(resp.json()["choices"][0]["message"]["content"])
            return {"document_type": content.get("document_type", "unknown"),
                    "confidence": content.get("confidence", 0.5)}
        return {"document_type": "unknown", "confidence": 0.0}

    # ────────────────────────────────────────────────────────────────────────
    # 3. Data Extraction (Document Intelligence)
    # ────────────────────────────────────────────────────────────────────────

    def extract_document_data(self, document_id: str, document_type: str,
                               file_bytes: bytes) -> List[Dict]:
        """Extract structured fields from document using appropriate DI model."""
        model_id = self.DI_MODELS.get(document_type, "prebuilt-layout")

        url = (f"{self.di_endpoint}/documentintelligence/documentModels/"
               f"{model_id}:analyze?api-version=2024-11-30")
        headers = {
            "Ocp-Apim-Subscription-Key": self.di_key,
            "Content-Type": "application/octet-stream",
        }

        resp = requests.post(url, headers=headers, data=file_bytes, timeout=120)
        if resp.status_code != 202:
            return []

        # Poll for result
        result_url = resp.headers.get("Operation-Location")
        import time
        for _ in range(60):
            time.sleep(2)
            result = requests.get(result_url,
                                  headers={"Ocp-Apim-Subscription-Key": self.di_key})
            result_json = result.json()
            if result_json.get("status") == "succeeded":
                return self._process_extraction_result(document_id, model_id, result_json)
            elif result_json.get("status") == "failed":
                break

        return []

    def _process_extraction_result(self, document_id: str, model_id: str,
                                    result: Dict) -> List[Dict]:
        """Process DI extraction result and store in Delta."""
        analyze_result = result.get("analyzeResult", {})
        extracted_fields = []

        # Process key-value pairs
        for kv in analyze_result.get("keyValuePairs", []):
            key = kv.get("key", {}).get("content", "")
            value = kv.get("value", {}).get("content", "")
            confidence = kv.get("confidence", 0.0)

            if key and value:
                field = {
                    "extraction_id": str(uuid.uuid4()),
                    "document_id": document_id,
                    "extraction_model": model_id,
                    "field_name": key,
                    "field_value": value,
                    "confidence": confidence,
                    "page_number": kv.get("key", {}).get("boundingRegions", [{}])[0].get("pageNumber", 1),
                }
                extracted_fields.append(field)

        # Process document-level fields
        for doc in analyze_result.get("documents", []):
            for field_name, field_data in doc.get("fields", {}).items():
                field = {
                    "extraction_id": str(uuid.uuid4()),
                    "document_id": document_id,
                    "extraction_model": model_id,
                    "field_name": field_name,
                    "field_value": str(field_data.get("valueString",
                                      field_data.get("content", ""))),
                    "field_type": field_data.get("type", "string"),
                    "confidence": field_data.get("confidence", 0.0),
                }
                extracted_fields.append(field)

        # Process tables
        for table_idx, table in enumerate(analyze_result.get("tables", [])):
            for cell in table.get("cells", []):
                field = {
                    "extraction_id": str(uuid.uuid4()),
                    "document_id": document_id,
                    "extraction_model": model_id,
                    "field_name": f"table_{table_idx}_r{cell.get('rowIndex')}_c{cell.get('columnIndex')}",
                    "field_value": cell.get("content", ""),
                    "confidence": cell.get("confidence", 0.0),
                    "page_number": cell.get("boundingRegions", [{}])[0].get("pageNumber", 1),
                }
                extracted_fields.append(field)

        # Persist to Delta
        if extracted_fields:
            schema = StructType([
                StructField("extraction_id", StringType()),
                StructField("document_id", StringType()),
                StructField("extraction_model", StringType()),
                StructField("field_name", StringType()),
                StructField("field_value", StringType()),
                StructField("field_type", StringType()),
                StructField("confidence", DoubleType()),
                StructField("page_number", IntegerType()),
            ])
            df = spark.createDataFrame(extracted_fields, schema)
            df.withColumn("created_at", current_timestamp()) \
              .write.mode("append") \
              .saveAsTable(f"{UNSTRUCTURED_SCHEMA}.document_extracted_data")

        # Update registry
        spark.sql(f"""
            UPDATE {UNSTRUCTURED_SCHEMA}.document_registry
            SET ai_extraction_completed = TRUE,
                processing_status = 'extracted',
                page_count = {analyze_result.get('pages', [{}]).__len__()},
                has_tables = {str(len(analyze_result.get('tables', [])) > 0).upper()},
                has_handwriting = {str(any(
                    s.get('kind') == 'handwriting'
                    for s in analyze_result.get('styles', [])
                )).upper()},
                processed_at = current_timestamp()
            WHERE document_id = '{document_id}'
        """)

        return extracted_fields

    # ────────────────────────────────────────────────────────────────────────
    # 4. Document Embedding & RAG
    # ────────────────────────────────────────────────────────────────────────

    def generate_embeddings(self, document_id: str, text_content: str,
                             chunk_size: int = 1000):
        """Generate embeddings for document chunks (for RAG/semantic search)."""
        # Chunk the text
        chunks = self._chunk_text(text_content, chunk_size)

        for idx, chunk in enumerate(chunks):
            embedding = self._get_embedding(chunk)
            if embedding:
                emb_id = str(uuid.uuid4())
                # Store as array<float> in Delta
                emb_str = ",".join([str(v) for v in embedding])
                spark.sql(f"""
                    INSERT INTO {UNSTRUCTURED_SCHEMA}.document_embeddings VALUES (
                        '{emb_id}', '{document_id}', {idx},
                        '{chunk.replace("'", "''")}',
                        ARRAY({emb_str}),
                        'text-embedding-ada-002',
                        {len(chunk.split())}, NULL, current_timestamp()
                    )
                """)

    def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding vector from Azure OpenAI."""
        url = (f"{self.openai_endpoint}/openai/deployments/"
               f"text-embedding-ada-002/embeddings?api-version=2024-06-01")
        headers = {"api-key": self.openai_key, "Content-Type": "application/json"}
        body = {"input": text[:8000]}  # Token limit
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        if resp.status_code == 200:
            return resp.json()["data"][0]["embedding"]
        return None

    def _chunk_text(self, text: str, chunk_size: int) -> List[str]:
        """Split text into overlapping chunks for embedding."""
        words = text.split()
        chunks = []
        overlap = chunk_size // 5
        i = 0
        while i < len(words):
            chunk = " ".join(words[i:i + chunk_size])
            chunks.append(chunk)
            i += chunk_size - overlap
        return chunks

    # ────────────────────────────────────────────────────────────────────────
    # 5. LLM Summarization
    # ────────────────────────────────────────────────────────────────────────

    def summarize_document(self, document_id: str, text_content: str,
                            document_type: str) -> Dict:
        """Generate AI summary with key entities and action items."""
        system_prompt = f"""You are an insurance document analyst. Summarize this {document_type} document.
        Return JSON with:
        - "executive_summary": 2-3 sentence summary
        - "key_findings": list of important findings
        - "entities": dict of named entities (people, dates, amounts, policy numbers, claim numbers)
        - "action_items": list of recommended next steps
        - "sentiment": overall tone (positive/neutral/negative)
        - "risk_flags": any red flags or concerns"""

        url = (f"{self.openai_endpoint}/openai/deployments/{self.openai_deployment}/"
               f"chat/completions?api-version=2024-06-01")
        headers = {"api-key": self.openai_key, "Content-Type": "application/json"}
        body = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text_content[:12000]}
            ],
            "max_tokens": 1500,
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }

        resp = requests.post(url, headers=headers, json=body, timeout=60)
        if resp.status_code == 200:
            summary = json.loads(resp.json()["choices"][0]["message"]["content"])
            summary_id = str(uuid.uuid4())

            spark.sql(f"""
                INSERT INTO {UNSTRUCTURED_SCHEMA}.document_summaries VALUES (
                    '{summary_id}', '{document_id}', 'executive',
                    '{summary.get("executive_summary", "").replace("'", "''")}',
                    '{json.dumps(summary.get("entities", {}))}',
                    '{summary.get("sentiment", "neutral")}',
                    '{json.dumps(summary.get("action_items", []))}',
                    '{self.openai_deployment}', NULL, current_timestamp()
                )
            """)
            return summary

        return {}

    # ────────────────────────────────────────────────────────────────────────
    # 6. Full Pipeline Orchestration
    # ────────────────────────────────────────────────────────────────────────

    def process_pending_documents(self, batch_size: int = 50):
        """Process all pending documents end-to-end."""
        pending = spark.sql(f"""
            SELECT document_id, source_path, document_type, domain
            FROM {UNSTRUCTURED_SCHEMA}.document_registry
            WHERE processing_status IN ('pending', 'classified')
            ORDER BY created_at ASC
            LIMIT {batch_size}
        """).collect()

        results = []
        for doc in pending:
            try:
                # Read file bytes from OneLake
                file_bytes = self._read_file_bytes(doc["source_path"])

                # Step 1: Classify if needed
                if doc["document_type"] == "pending_classification":
                    classification = self.classify_document(doc["document_id"], file_bytes)
                    doc_type = classification["document_type"]
                else:
                    doc_type = doc["document_type"]

                # Step 2: Extract structured data
                fields = self.extract_document_data(doc["document_id"], doc_type, file_bytes)

                # Step 3: Get full text (OCR)
                full_text = self._get_full_text(file_bytes)

                if full_text:
                    # Step 4: Generate embeddings
                    self.generate_embeddings(doc["document_id"], full_text)

                    # Step 5: Summarize
                    summary = self.summarize_document(doc["document_id"], full_text, doc_type)

                    # Step 6: PII detection
                    self._detect_pii(doc["document_id"], full_text)

                # Mark completed
                spark.sql(f"""
                    UPDATE {UNSTRUCTURED_SCHEMA}.document_registry
                    SET processing_status = 'completed', processed_at = current_timestamp()
                    WHERE document_id = '{doc["document_id"]}'
                """)

                results.append({"document_id": doc["document_id"], "status": "completed",
                                "fields_extracted": len(fields)})

            except Exception as e:
                spark.sql(f"""
                    UPDATE {UNSTRUCTURED_SCHEMA}.document_registry
                    SET processing_status = 'failed'
                    WHERE document_id = '{doc["document_id"]}'
                """)
                results.append({"document_id": doc["document_id"], "status": "failed",
                                "error": str(e)})

        print(f"📄 Processed {len(results)} documents: "
              f"{sum(1 for r in results if r['status'] == 'completed')} succeeded, "
              f"{sum(1 for r in results if r['status'] == 'failed')} failed")
        return results

    # ────────────────────────────────────────────────────────────────────────
    # Helpers
    # ────────────────────────────────────────────────────────────────────────

    def _read_file_bytes(self, path: str) -> bytes:
        """Read file bytes from OneLake."""
        binary_df = spark.read.format("binaryFile").load(path)
        return binary_df.first()["content"]

    def _get_full_text(self, file_bytes: bytes) -> Optional[str]:
        """Get full OCR text via Document Intelligence layout model."""
        url = (f"{self.di_endpoint}/documentintelligence/documentModels/"
               f"prebuilt-layout:analyze?api-version=2024-11-30")
        headers = {
            "Ocp-Apim-Subscription-Key": self.di_key,
            "Content-Type": "application/octet-stream",
        }
        resp = requests.post(url, headers=headers, data=file_bytes, timeout=120)
        if resp.status_code == 202:
            import time
            result_url = resp.headers.get("Operation-Location")
            for _ in range(60):
                time.sleep(2)
                result = requests.get(result_url,
                                      headers={"Ocp-Apim-Subscription-Key": self.di_key})
                if result.json().get("status") == "succeeded":
                    return result.json().get("analyzeResult", {}).get("content", "")
        return None

    def _detect_pii(self, document_id: str, text: str):
        """Detect PII in document text using Azure AI Language."""
        # Use Azure AI Language PII detection
        url = f"{self.di_endpoint}/language/:analyze-text/pii-entities?api-version=2023-04-01"
        headers = {
            "Ocp-Apim-Subscription-Key": self.di_key,
            "Content-Type": "application/json"
        }
        body = {
            "analysisInput": {"documents": [{"id": "1", "language": "en", "text": text[:5000]}]},
            "parameters": {"domain": "phi"}  # Healthcare PII
        }
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        if resp.status_code == 200:
            pii_entities = resp.json().get("results", {}).get("documents", [{}])[0].get("entities", [])
            pii_types = list(set([e["category"] for e in pii_entities]))
            if pii_types:
                spark.sql(f"""
                    UPDATE {UNSTRUCTURED_SCHEMA}.document_registry
                    SET pii_detected = TRUE,
                        pii_elements = '{json.dumps(pii_types)}'
                    WHERE document_id = '{document_id}'
                """)

    def _get_config(self, key: str) -> str:
        row = spark.sql(f"""
            SELECT config_value FROM {METADATA_SCHEMA}.environment_config
            WHERE config_key = '{key}' AND environment = '{self.environment}' AND is_active = TRUE
            LIMIT 1
        """).first()
        return row["config_value"] if row else ""

    def _get_secret(self, secret_name: str) -> str:
        # In Fabric: notebookutils.credentials.getSecret(kv_url, secret_name)
        return f"resolved:{secret_name}"


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    create_unstructured_tables()

    engine = DocumentAIEngine(environment="prod")

    # Ingest from OneLake
    # engine.ingest_documents_from_onelake(
    #     "abfss://ws@onelake.dfs.fabric.microsoft.com/lh_claims/Files/documents/",
    #     domain="claims", related_entity_type="claim"
    # )

    # Process pending
    # engine.process_pending_documents(batch_size=20)
