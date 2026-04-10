# 🎯 Golden Record & Address Standardization - Implementation Guide

## Current Status

### ✅ What's Already Implemented (Notebook 30)

**Basic Deduplication:**
- Row-level deduplication by business key
- Keep latest record based on ingestion time
- SCD Type 2 tracking for history

**Current Logic:**
```python
# Simple deduplication - keeps latest record
window_dedup = Window.partitionBy(business_key).orderBy(desc("_ingestion_time"))
df_deduped = df_bronze \
    .withColumn("_row_num", row_number().over(window_dedup)) \
    .filter(col("_row_num") == 1)
```

### ⚠️ What Needs Enhancement

**Missing from Architecture:**
1. **Fuzzy matching** for identity resolution
2. **Probabilistic matching** on name, DOB, SSN, address
3. **Golden record creation** with multi-source merge
4. **Address standardization** with external API integration
5. **Customer 360** master record construction

---

## 📍 Where to Implement

### Location 1: Notebook 30 - Medallion Transformations

**Add new sections for Customer MDM:**
- Section 2A: Customer Identity Resolution (fuzzy matching)
- Section 2B: Address Standardization (USPS/postal API)
- Section 3A: Golden Record Creation (Gold layer)

### Location 2: New Notebook (Optional)

**Create: `32_customer_mdm_golden_record.ipynb`**
- Dedicated Customer MDM notebook
- Advanced matching algorithms
- Address standardization service
- Golden record assembly

---

## 🔧 Implementation Code

### 1. Identity Resolution with Fuzzy Matching

Add to Notebook 30 (or new notebook 32):

```python
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Customer Identity Resolution — Fuzzy Matching                      ║
# ╚══════════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from fuzzywuzzy import fuzz  # or use Spark's built-in Levenshtein
from pyspark.sql.types import IntegerType

# === STEP 1: Load customer data from multiple sources ===

df_customer_crm = spark.table("bronze.customers_crm")
df_customer_policy = spark.table("bronze.customers_policy_system")
df_customer_claims = spark.table("bronze.customers_claims_portal")

# Union all sources with source identifier
df_customers_all = df_customer_crm.withColumn("source_system", lit("CRM")) \
    .union(df_customer_policy.withColumn("source_system", lit("POLICY_SYSTEM"))) \
    .union(df_customer_claims.withColumn("source_system", lit("CLAIMS_PORTAL")))

print(f"Total customer records from all sources: {df_customers_all.count()}")

# === STEP 2: Standardize fields for matching ===

df_standardized = df_customers_all \
    .withColumn("name_clean", 
        regexp_replace(
            upper(trim(col("customer_name"))), 
            r"[^A-Z0-9\s]", 
            ""
        )
    ) \
    .withColumn("ssn_clean", 
        regexp_replace(col("ssn"), r"[^0-9]", "")
    ) \
    .withColumn("dob_clean", 
        to_date(col("date_of_birth"), "yyyy-MM-dd")
    ) \
    .withColumn("address_clean",
        regexp_replace(
            upper(trim(concat_ws(" ", col("address_line1"), col("city"), col("state")))),
            r"[^A-Z0-9\s]",
            ""
        )
    )

# === STEP 3: Define matching rules ===

def calculate_match_score(row1, row2):
    """
    Calculate probabilistic match score between two customer records.
    
    Scoring:
    - SSN exact match: +40 points
    - DOB exact match: +20 points
    - Name fuzzy match (>80%): +20 points
    - Address fuzzy match (>70%): +15 points
    - Email match: +5 points
    
    Total possible: 100 points
    Threshold: 70+ points = MATCH
    """
    score = 0
    
    # SSN match (strongest signal)
    if row1.ssn_clean == row2.ssn_clean and row1.ssn_clean:
        score += 40
    
    # DOB match
    if row1.dob_clean == row2.dob_clean and row1.dob_clean:
        score += 20
    
    # Name fuzzy match
    name_ratio = fuzz.ratio(row1.name_clean, row2.name_clean)
    if name_ratio >= 80:
        score += 20
    elif name_ratio >= 60:
        score += 10
    
    # Address fuzzy match
    address_ratio = fuzz.ratio(row1.address_clean, row2.address_clean)
    if address_ratio >= 70:
        score += 15
    elif address_ratio >= 50:
        score += 7
    
    # Email match
    if row1.email and row2.email and row1.email.lower() == row2.email.lower():
        score += 5
    
    return score

# Register UDF for fuzzy matching
@udf(returnType=IntegerType())
def fuzzy_match_score(name1, name2):
    """Calculate Levenshtein-based match score for names."""
    if not name1 or not name2:
        return 0
    return fuzz.ratio(str(name1), str(name2))

# === STEP 4: Create candidate pairs (blocking strategy) ===
# Block 1: Same SSN (strongest match)
# Block 2: Same DOB + Similar Name (first 3 chars)
# Block 3: Same last name + Same ZIP code

df_standardized = df_standardized.withColumn("customer_key", monotonically_increasing_id())

# Block 1: SSN-based candidates
df_ssn_block = df_standardized.alias("a") \
    .join(
        df_standardized.alias("b"),
        (col("a.ssn_clean") == col("b.ssn_clean")) & 
        (col("a.ssn_clean").isNotNull()) &
        (col("a.customer_key") < col("b.customer_key")),  # Avoid duplicates
        "inner"
    ) \
    .select(
        col("a.customer_key").alias("key_a"),
        col("b.customer_key").alias("key_b"),
        lit("SSN_MATCH").alias("block_type")
    )

# Block 2: DOB + Name prefix
df_dob_name_block = df_standardized.alias("a") \
    .join(
        df_standardized.alias("b"),
        (col("a.dob_clean") == col("b.dob_clean")) &
        (substring(col("a.name_clean"), 1, 3) == substring(col("b.name_clean"), 1, 3)) &
        (col("a.dob_clean").isNotNull()) &
        (col("a.customer_key") < col("b.customer_key")),
        "inner"
    ) \
    .select(
        col("a.customer_key").alias("key_a"),
        col("b.customer_key").alias("key_b"),
        lit("DOB_NAME_PREFIX").alias("block_type")
    )

# Union all candidate pairs
df_candidate_pairs = df_ssn_block.union(df_dob_name_block).distinct()

print(f"Candidate pairs for matching: {df_candidate_pairs.count()}")

# === STEP 5: Score candidate pairs ===

# Join back to get full record details
df_pairs_detailed = df_candidate_pairs \
    .join(df_standardized.alias("a"), col("key_a") == col("a.customer_key")) \
    .join(df_standardized.alias("b"), col("key_b") == col("b.customer_key"))

# Calculate match scores using UDFs
df_scored = df_pairs_detailed \
    .withColumn("name_score", fuzzy_match_score(col("a.name_clean"), col("b.name_clean"))) \
    .withColumn("ssn_match", when(col("a.ssn_clean") == col("b.ssn_clean"), 40).otherwise(0)) \
    .withColumn("dob_match", when(col("a.dob_clean") == col("b.dob_clean"), 20).otherwise(0)) \
    .withColumn("address_score", fuzzy_match_score(col("a.address_clean"), col("b.address_clean")) * 0.15) \
    .withColumn("total_score", 
        col("ssn_match") + col("dob_match") + 
        when(col("name_score") >= 80, 20).when(col("name_score") >= 60, 10).otherwise(0) +
        col("address_score")
    )

# Filter by match threshold (70+ = match)
df_matches = df_scored.filter(col("total_score") >= 70)

print(f"High-confidence matches found: {df_matches.count()}")

# === STEP 6: Create match clusters (transitive closure) ===

# Use GraphFrames or iterative approach to find connected components
# For simplicity, using SQL window function approach

df_clusters = df_matches \
    .select(
        col("key_a").alias("customer_key")
    ).union(
        df_matches.select(col("key_b").alias("customer_key"))
    ).distinct()

# Assign cluster IDs (simplified - in production use GraphFrames)
window_cluster = Window.orderBy("customer_key")
df_clusters = df_clusters.withColumn("golden_record_id", row_number().over(window_cluster))

# Map all matched keys to their golden record
df_golden_mapping = df_matches \
    .join(df_clusters, df_matches.key_a == df_clusters.customer_key) \
    .select(
        col("key_a").alias("source_key"),
        col("golden_record_id")
    ).union(
        df_matches \
            .join(df_clusters, df_matches.key_b == df_clusters.customer_key) \
            .select(
                col("key_b").alias("source_key"),
                col("golden_record_id")
            )
    ).distinct()

# Assign golden_record_id to all customer records
df_with_golden_id = df_standardized \
    .join(df_golden_mapping, df_standardized.customer_key == df_golden_mapping.source_key, "left") \
    .withColumn("golden_record_id", 
        when(col("golden_record_id").isNull(), col("customer_key")).otherwise(col("golden_record_id"))
    )

print("✅ Identity resolution complete - golden_record_id assigned to all records")

# Save to Silver layer
df_with_golden_id.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customer.customers_matched")
```

---

### 2. Address Standardization

```python
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Address Standardization with External API                          ║
# ╚══════════════════════════════════════════════════════════════════════╝

import requests
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd

# === OPTION 1: USPS API Integration ===

def standardize_address_usps(address_line1, city, state, zip_code):
    """
    Call USPS Address Validation API to standardize address.
    
    Returns:
        - standardized_address_line1
        - standardized_city
        - standardized_state
        - standardized_zip
        - is_validated (boolean)
    """
    # USPS API endpoint (requires API key)
    # https://www.usps.com/business/web-tools-apis/address-information-api.htm
    
    usps_api_url = "https://secure.shippingapis.com/ShippingAPI.dll"
    usps_user_id = "YOUR_USPS_USER_ID"  # Get from USPS
    
    xml_request = f"""
    <AddressValidateRequest USERID="{usps_user_id}">
        <Address>
            <Address1></Address1>
            <Address2>{address_line1}</Address2>
            <City>{city}</City>
            <State>{state}</State>
            <Zip5>{zip_code[:5]}</Zip5>
            <Zip4></Zip4>
        </Address>
    </AddressValidateRequest>
    """
    
    try:
        response = requests.post(
            usps_api_url,
            params={"API": "Verify", "XML": xml_request},
            timeout=5
        )
        
        if response.status_code == 200:
            # Parse XML response (simplified)
            # In production, use xml.etree.ElementTree
            return {
                "standardized_address_line1": address_line1,  # Extract from XML
                "standardized_city": city,
                "standardized_state": state,
                "standardized_zip": zip_code,
                "is_validated": True
            }
        else:
            return {
                "standardized_address_line1": address_line1,
                "standardized_city": city,
                "standardized_state": state,
                "standardized_zip": zip_code,
                "is_validated": False
            }
    except Exception as e:
        # Return original if validation fails
        return {
            "standardized_address_line1": address_line1,
            "standardized_city": city,
            "standardized_state": state,
            "standardized_zip": zip_code,
            "is_validated": False
        }

# === OPTION 2: Spark UDF for Basic Standardization ===

@udf(returnType=StructType([
    StructField("standardized_line1", StringType()),
    StructField("standardized_city", StringType()),
    StructField("standardized_state", StringType()),
    StructField("standardized_zip", StringType())
]))
def standardize_address_basic(address_line1, city, state, zip_code):
    """
    Basic address standardization using Spark UDF (no external API).
    
    Rules:
    - Convert to uppercase
    - Expand abbreviations (ST -> STREET, AVE -> AVENUE)
    - Remove special characters
    - Standardize ZIP format (12345-6789 or 12345)
    """
    if not address_line1:
        return None
    
    # Standardize address line
    addr = address_line1.upper().strip()
    
    # Expand abbreviations
    abbrev_map = {
        " ST$": " STREET",
        " ST ": " STREET ",
        " AVE$": " AVENUE",
        " AVE ": " AVENUE ",
        " BLVD$": " BOULEVARD",
        " BLVD ": " BOULEVARD ",
        " DR$": " DRIVE",
        " DR ": " DRIVE ",
        " RD$": " ROAD",
        " RD ": " ROAD ",
        " LN$": " LANE",
        " LN ": " LANE ",
        " CT$": " COURT",
        " CT ": " COURT ",
        " PL$": " PLACE",
        " PL ": " PLACE ",
        " APT ": " APARTMENT ",
        " STE ": " SUITE "
    }
    
    import re
    for abbrev, full in abbrev_map.items():
        addr = re.sub(abbrev, full, addr)
    
    # Standardize city
    city_std = city.upper().strip() if city else None
    
    # Standardize state (2-letter code)
    state_std = state.upper().strip() if state else None
    
    # Standardize ZIP (format: 12345 or 12345-6789)
    zip_std = None
    if zip_code:
        zip_clean = re.sub(r'[^0-9]', '', str(zip_code))
        if len(zip_clean) >= 5:
            zip_std = zip_clean[:5]
            if len(zip_clean) == 9:
                zip_std += "-" + zip_clean[5:9]

    return (addr, city_std, state_std, zip_std)

# === Apply address standardization ===

df_customers = spark.table("silver_customer.customers_matched")

df_standardized_addresses = df_customers \
    .withColumn("address_std", 
        standardize_address_basic(
            col("address_line1"),
            col("city"),
            col("state"),
            col("zip_code")
        )
    ) \
    .withColumn("standardized_address_line1", col("address_std.standardized_line1")) \
    .withColumn("standardized_city", col("address_std.standardized_city")) \
    .withColumn("standardized_state", col("address_std.standardized_state")) \
    .withColumn("standardized_zip", col("address_std.standardized_zip")) \
    .drop("address_std")

print("✅ Address standardization complete")

# Save
df_standardized_addresses.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customer.customers_standardized")
```

---

### 3. Golden Record Creation (Gold Layer)

```python
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Golden Record Assembly — Master Customer Record                    ║
# ╚══════════════════════════════════════════════════════════════════════╝

# === STEP 1: Load matched and standardized customers ===

df_silver = spark.table("silver_customer.customers_standardized")

# === STEP 2: Define survivorship rules ===
# For each golden_record_id cluster, pick the "best" value for each attribute

# Survivorship rules:
# - Most recent record for dynamic fields (phone, email, address)
# - Most complete record for static fields (SSN, DOB, name)
# - Highest quality source system (CRM > POLICY > CLAIMS)

# Source system priority
source_priority = {
    "CRM": 3,
    "POLICY_SYSTEM": 2,
    "CLAIMS_PORTAL": 1
}

from pyspark.sql.functions import create_map, lit

# Create priority map
source_map = create_map([lit(x) for x in sum(source_priority.items(), ())])

# === STEP 3: Apply survivorship rules ===

# Add source priority
df_with_priority = df_silver.withColumn(
    "source_priority",
    source_map[col("source_system")]
)

# Window for golden record selection
window_golden = Window.partitionBy("golden_record_id").orderBy(
    desc("source_priority"),    # Prefer CRM source
    desc("_ingestion_time"),    # Then most recent
    desc("customer_key")         # Then stable sort
)

# Select best record for each golden_record_id
df_golden_records = df_with_priority \
    .withColumn("_rank", row_number().over(window_golden)) \
    .filter(col("_rank") == 1) \
    .drop("_rank")

# === STEP 4: Create comprehensive master record ===

df_customer_master = df_golden_records.select(
    col("golden_record_id").alias("customer_master_id"),
    col("customer_name").alias("master_customer_name"),
    col("ssn_clean").alias("master_ssn"),
    col("dob_clean").alias("master_date_of_birth"),
    col("standardized_address_line1").alias("master_address_line1"),
    col("standardized_city").alias("master_city"),
    col("standardized_state").alias("master_state"),
    col("standardized_zip").alias("master_zip_code"),
    col("email").alias("master_email"),
    col("phone").alias("master_phone"),
    col("source_system").alias("master_source_system"),
    current_timestamp().alias("master_created_date"),
    current_timestamp().alias("master_updated_date"),
    lit(True).alias("is_active")
)

print(f"Golden records created: {df_customer_master.count()}")

# === STEP 5: Create cross-reference table (XRef) ===
# Maps source system IDs to golden record ID

df_xref = df_silver.select(
    col("golden_record_id").alias("customer_master_id"),
    col("source_system"),
    col("customer_id").alias("source_system_customer_id"),
    col("customer_key").alias("source_record_key"),
    current_timestamp().alias("xref_created_date")
)

# === STEP 6: Save to Gold layer ===

# Main golden record table
df_customer_master.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_customer.dim_customer_master")

# Cross-reference table
df_xref.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer.customer_xref")

print("✅ Golden record master table created → gold_customer.dim_customer_master")
print("✅ Cross-reference table created → gold_customer.customer_xref")

# === STEP 7: Create address dimension ===

df_address_dimension = df_customer_master.select(
    monotonically_increasing_id().alias("address_key"),
    col("master_address_line1").alias("address_line1"),
    col("master_city").alias("city"),
    col("master_state").alias("state"),
    col("master_zip_code").alias("zip_code"),
    lit("USA").alias("country")
).distinct()

df_address_dimension.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer.dim_address")

print("✅ Address dimension created → gold_customer.dim_address")
```

---

## 📊 Expected Results

### Tables Created:

1. **silver_customer.customers_matched**
   - All customer records with `golden_record_id`
   - Match scores and cluster assignments

2. **silver_customer.customers_standardized**
   - Standardized addresses (USPS or basic rules)
   - Cleaned and normalized fields

3. **gold_customer.dim_customer_master**
   - One record per unique customer (golden record)
   - Best values selected via survivorship rules
   - ~70-80% reduction from source records

4. **gold_customer.customer_xref**
   - Maps source system IDs → golden record ID
   - For reverse lookups and lineage

5. **gold_customer.dim_address**
   - Unique addresses for dimensional modeling

---

## 🎯 Integration with Existing Notebooks

### Update Notebook 30:
Add these sections after existing Silver logic:
- **Section 2A**: Customer identity resolution (lines 300-500)
- **Section 2B**: Address standardization (lines 500-600)
- **Section 3A**: Golden record creation in Gold (lines 700-900)

### Update Notebook 90 (Dashboard):
Add Customer 360 view:
```python
# Customer 360 - Join golden record with all transactions
df_customer360 = spark.table("gold_customer.dim_customer_master").alias("c") \
    .join(spark.table("gold_policy.fact_policy_transaction").alias("p"), 
          col("c.customer_master_id") == col("p.customer_id"), "left") \
    .join(spark.table("gold_claims.fact_claim").alias("cl"),
          col("c.customer_master_id") == col("cl.customer_id"), "left") \
    .groupBy("c.customer_master_id", "c.master_customer_name") \
    .agg(
        count("p.policy_id").alias("total_policies"),
        sum("p.premium_amount").alias("total_premium"),
        count("cl.claim_id").alias("total_claims"),
        sum("cl.claim_amount").alias("total_claim_amount")
    )
```

---

## 🚀 Next Steps

1. **Implement in Notebook 30** (or create new Notebook 32)
2. **Test with sample data** from Notebook 01
3. **Validate match quality** - review match scores
4. **Configure USPS API** - get API key for production
5. **Add to Notebook 60** - create unit tests for matching logic
6. **Update Dashboard** (Notebook 90) - add Customer 360 view

---

## 📚 References

- **Architecture Section**: 2.2 Customer/Party (MDM)
- **Identity Resolution**: Probabilistic matching on name, DOB, SSN, address
- **Address Standardization**: Spark UDF + external API (USPS)
- **Golden Record**: `dim_customer_master` in Gold lakehouse
- **Customer 360**: Unified view across Policy, Claims, Finance

---

**Status**: Architecture defined, basic deduplication in place, advanced MDM ready to implement.
