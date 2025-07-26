## 🔄 Transformation Phase (Detailed)

The transformation phase processes raw flight pricing JSON data into structured, analysis-ready tables using PySpark within a notebook-driven, modular framework. All transformation logic follows medallion architecture principles (Bronze → Silver → Gold).

---

### 🔐 1. Secure Access to ADLS via OAuth2 & Key Vault
- **Purpose**: To securely access Azure Data Lake Storage Gen2 from Databricks without exposing credentials.
- **Implementation**:
  - Created secrets in **Azure Key Vault** (Client ID, Secret, Tenant ID, etc.)
  - Configured **Databricks Secret Scope** to access Key Vault
  - Used **OAuth2 token-based authentication** within Spark config for enterprise-grade security
- **Outcome**: Credentials are securely abstracted and access to ADLS is fully automated via token refresh.

---

### 📂 2. Data Loading & Merging (Bronze to Silver)
- Read monthly partitioned raw JSON files from ADLS (Bronze layer).
- Used `For` loop in PySpark to dynamically build file paths for 12 months of data per route.
- Applied `.read.format("json")` with schema inference.
- Skipped missing or corrupted files using `try-except` logic.
- Appended all valid DataFrames into one unified frame using `.union()`.

---

### 🧪 3. Data Cleansing & Standardization
- **Removed duplicates** based on route + departure date + fare class.
- Applied schema validation to enforce expected fields:
  - `origin`, `destination`, `departureDate`, `currency`, `priceMetrics`, etc.
- Handled missing/null values in critical fields:
  - Dropped rows missing fare class prices
  - Applied `na.drop()` or default imputation as needed

---

### 📊 4. JSON Parsing & Nested Structure Flattening
- Parsed complex arrays and nested fields using:
  - `explode()` for arrays inside the `data` field
  - `selectExpr()` and `withColumn()` for field extraction
- Extracted and flattened:
  - `CurrencyCode`, `Departure_Date`, `Origin`, `Destination`, `OneWay`
  - Fares for `Economy`, `PremiumEconomy`, `Business`, `First` from `priceMetrics.amount`

---

### ✅ 5. Data Quality Check – Redundancy & Conditional Save
- Created custom function `chkRedundancy` to detect:
  - Duplicate departure dates
  - Missing values in any fare class column
- Conditional logic:
  - If data passed quality checks → saved to `VerifiedData` path in Silver layer
  - If not → logged and skipped
- Ensured only clean, validated datasets are pushed downstream.

---

### 🗓️ 6. Holiday Calendar Integration
- Created **custom UDFs** to enrich data with holiday features:
  - `is_holiday_udf(departure_date, country)` → returns Boolean
  - `holiday_name_udf(departure_date, country)` → returns holiday name string
- Applied to both **origin** and **destination** IATA codes for:
  - UK, France, Netherlands, Spain, Italy
- New Columns Added:
  - `is_origin_holiday`, `is_destination_holiday`
  - `origin_holiday_name`, `destination_holiday_name`
  - `is_holiday_route` (combined flag)

---

### 💾 7. Writing to Silver Layer in Delta Format
- Saved final validated DataFrame as **Delta Table** using:
  ```python
  df.write.format("delta").mode("overwrite").save(path)
