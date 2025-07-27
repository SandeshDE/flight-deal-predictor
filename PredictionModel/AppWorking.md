## 🚀 How the App Works

This application is a web-based **Flight Deal Predictor** that allows users to check if a flight ticket price is a good deal, based on historical trends and machine learning forecasts.

---

### 🛠️ Technologies Used
- **Python Flask** – For the backend web server
- **HTML (Jinja templates)** – For rendering input forms and results
- **Scikit-learn** – For ML model (RandomForestRegressor)
- **Matplotlib** – For visualizing forecasted prices
- **Google Drive** – For storing and retrieving trained model artifacts

---

### 🧩 App Workflow Overview

1. **User Input via Web Form**
   - The user provides:
     - Origin (e.g., LHR)
     - Destination (e.g., CDG)
     - Fare Class (Economy, Business, etc.)
     - Booking Date
     - Travel Date
     - Ticket Price

2. **Preprocessing & Feature Engineering**
   - The app calculates:
     - `days_until_departure`
     - Day of week, month, year
     - Holiday flag (based on origin/destination date)

3. **Model Prediction**
   - The appropriate ML model (`.pkl` file) is loaded based on the selected fare class
   - The model forecasts price increase percentages for:
     - 0, 15, 45, 60, and 90 days before departure
   - It calculates the **optimal fare price**

4. **Deal Evaluation**
   - Compares user-entered price to predicted optimal price
   - If user price < optimal → **“Good Deal”**
   - Otherwise → **“Not a Good Deal”**

5. **Visualization**
   - Generates a line chart with:
     - X-axis: Days Before Departure
     - Y-axis: Predicted Price
   - Chart is rendered on the result page with a table of forecasts

---

### 📥 Download & Run Locally

1. **Download the full app (with trained models):**  
   [Google Drive – Python App ZIP](https://drive.google.com/uc?export=download&id=1XnQXcHFUxOejcGcuf632IMGE8teC-Xkb)

2. **Unzip the file and open terminal in the project folder**

3. **Create a virtual environment (optional):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate

