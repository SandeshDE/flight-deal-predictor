# Flight Deal Predictor – End-to-End Data Engineering & Forecasting System

## 🔍 Project Vision and Goal

Build a cloud-agnostic, scalable ETL and ML pipeline to forecast flight fare prices and classify ticket deals. The system:

* Uses historical pricing data via the Amadeus API
* Implements a robust ETL pipeline
* Transforms and models data to output actionable fare predictions
* Powers a web app that classifies deals as "Good" or "Not a Good Deal"

routes: LHR-CDG, CDG-AMS, LHR-AMS, BCN-FCO



---

## 📊 Data Source: Amadeus API

* Used the Amadeus Flight PriceSearch API
* Data includes: origin, destination, departure date, class-based pricing (economy, business, etc.)
* REST API with OAuth2 authentication
* Supports pagination, filtering, and parameterization
  <img width="629" height="120" alt="image" src="https://github.com/user-attachments/assets/82eb0525-4de9-46fc-a060-93727c19f137" />


---

## ⚖️ Medallion Architecture

* **Bronze Layer**: Raw API data (unvalidated)
* **Silver Layer**: Cleaned, parsed JSON (validated records)
* **Gold Layer**: Aggregated and structured data (Star Schema, ML-ready)
* <img width="895" height="336" alt="image" src="https://github.com/user-attachments/assets/1af30399-4a7a-4ae5-8386-6e890b397a4a" />


---

## ♻️ Ingestion Workflow (Azure Data Factory)

**Pipeline Name**: `EXTRACTION_FLIGHTS`

* Retrieve Secrets → Azure Key Vault
* Generate Access Token → Web activity
* Loop over Dates → ForEach loop
* Set Date Dynamically → SetVariable
* Call Amadeus API → Copy activity
* Write to ADLS Gen2 → JSON format, partitioned by month/year

**Copy Activity**:

* Source: REST API (GET), OAuth Bearer Token, pagination supported
* Sink: JSON to ADLS Gen2
* Retry: 10x, Timeout: 12 hrs, Interval: 30s

  <img width="933" height="588" alt="image" src="https://github.com/user-attachments/assets/be5ed89e-4867-467a-afe6-9e3d9ba90d52" />


---

## ⚖️ Transformation Phase (Databricks / PySpark)

### Secure Access via OAuth + Key Vault

* Secrets managed via Key Vault
* Databricks secret scope integration
* Credentials never exposed in code

### Data Loading and Merging

* Loop through 12 months per route/year
* Read partitioned JSON files dynamically
* Skip missing/corrupted files with try-except
* Merge valid DataFrames using `.union()`

### Writing to Delta Lake

* Save merged DataFrame to Delta using `.write.format("delta")`
* Overwrite mode with dynamic paths based on origin/destination/year
* Silver path: `abfss://silver@flightsg.dfs.core.windows.net/Cumulative_routes/{origin}-{destination}/{year}`

### Delta Ingestion

* Access Silver layer Delta files using widgets for origin/destination
* Merge across 2023 and 2024
* Fault-tolerant merging via try-except

### JSON Transformation and Flattening

* Use `explode()` to parse array data
* Extract nested fields: currencyCode, departureDate, pricing per class
* Final structured schema generated for validation

### Redundancy Check

* Custom `chkRedundancy` function:

  * Detects duplicate records and nulls in critical columns
  * Saves only clean datasets to Verified path:
  * `abfss://silver@flightsg.dfs.core.windows.net/VerifiedData/route:{origin}-{destination}`

### Holiday Calendar Integration

* Applied holiday UDFs:

  * `is_holiday_udf`, `holiday_name_udf`
* Added:

  * `is_origin_holiday`, `is_destination_holiday`
  * `origin_holiday_name`, `destination_holiday_name`
* Derived columns:

  * `is_holiday_route`, `holiday_name`

---

## 📈 Data Modeling – Star Schema

* Fact Table: `Fact_Flight`
* Dimension Tables:

  * `Dim_Date`: Date breakdowns
  * `Dim_IATA`: Origin/destination metadata
  * `Dim_Currency`: Currency info
  * `Dim_Holiday`: Holiday mappings
* Schema maintained for optimal querying

  <img width="1704" height="975" alt="image" src="https://github.com/user-attachments/assets/075d95c4-89be-41f2-a58c-7a368e5c2e17" />


---

## 📁 Optimized Data Export Workflow(Loading)

* Due to cloud storage cost, transitioned from Delta to Parquet format
* Parquet = schema preservation + compression + SQL Server compatibility
* Export Path: `abfss://gold@flightsg.dfs.core.windows.net/StarSchema/FactsParquet/Fact_Flight:{origin}-{destination}`
* Parquet files ingested into SQL Server with type safety and integrity
  <img width="1788" height="725" alt="image" src="https://github.com/user-attachments/assets/f3985520-84f1-411e-817e-4b30ffe9bebf" />

---

## 🧪 ML Pipeline & Forecasting Logic

### ML Flow

* Extract from SQL Server
* Preprocess: parse dates, handle missing, generate features
* Model: One RandomForestRegressor per fare class
* Export models as `.pkl` files

### Feature Engineering

* `days_until_departure`, `month`, `day_of_week`, `holiday_flag`
* `pct_change_*`: price deviation from monthly average
* Forecast logic:

  * `forecasted_increase = clip(pct_change + holiday_adjustment)`
  * `optimal_price = base_price * (1 + forecasted_increase)`

### Training Details

* 80/20 split, 100 trees, `random_state=42`
* `SimpleImputer` + `StandardScaler` + `RandomForestRegressor`

### Output

* `.pkl` files saved in: `model/{fare}_model.pkl`
* Use cases: fare tools, alerts, pricing dashboards

---

## 💻 Web Application – Flask UI

### Architecture

* **Frontend**: `form.html` collects user input
* **Backend**: `app.py` loads models and performs predictions
* **Visualization**: Matplotlib chart with forecasted prices
* **Data**: SQL Server queries

### Inputs

* Origin, Destination, Fare Class, Booking Date, Travel Date, Price

### Forecast Logic

* For each forecast window: 0, 15, 45, 60, 90 days
* Compute: `days_until_departure`, `holiday_flag`, `month`, `day_of_week`
* Predict price increase %, compute `optimal_price`

### Output

* Forecasted prices by date range
* "Good Deal" if user price < optimal price
* Rendered via `result.html` template
* Line chart: Days Before Departure vs. Predicted Price

<img width="796" height="1045" alt="image" src="https://github.com/user-attachments/assets/b036aaa8-4ff8-4756-8c30-0ae1daae20ee" />

---

## 📆 Author

**SAISANDESH DEVIREDDY**
Email: sandeshwillbe@gmail.com
GitHub: [https://github.com/SandeshDE](https://github.com/SandeshDE)

---

## 📄 License

This project is licensed under the MIT License.
