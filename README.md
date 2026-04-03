# 🛡️ Data Quality Firewall
**An Automated, Event-Driven Data Validation Pipeline**

<p align="center">
  <img src="architecture.png" width="850" alt="Data Quality Firewall Architecture">
</p>

---

The **Data Quality Firewall** is a robust Python-based "gatekeeper" designed to automate the ingestion and validation of CSV data into a **MySQL** data warehouse. It eliminates manual data entry errors by ensuring only high-quality, verified records reach your production environment.

### 🚀 Core Architecture
*   **Real-Time Monitoring:** Built with the `watchdog` library, the system acts as a file observer, instantly detecting new CSV uploads in the `incoming/` directory.
*   **Parallel Processing:** Utilizing a `ThreadPoolExecutor`, the firewall processes multiple files simultaneously and runs concurrent quality checks (null-value detection and outlier analysis) to maximize throughput.
*   **Automated Promotion:** If a file passes all quality gates, its data is automatically appended to the **MySQL** `employee_demographics` table via **SQLAlchemy** and moved to a `processed/` folder.
*   **Fail-Safe Reporting:** Rejected files stay in the `incoming/` directory, while the system generates a detailed **PDF summary report** (using `ReportLab`) and an **SQLite** audit log explaining exactly which rows or columns failed validation.

### 🛠️ Tech Stack
*   **Language:** Python 3.x
*   **Data Handling:** Pandas, SQLAlchemy, PyMySQL
*   **Automation:** Watchdog (File System Events)
*   **Reporting:** ReportLab (PDF Generation)
*   **Storage:** MySQL (Production Warehouse) & SQLite (Audit Logs)

---

### 📦 Setup & Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com
