from flask import Flask, request, jsonify
import pandas as pd
from pyxecm import OTCS
from hdbcli import dbapi
import os
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ---------------- CONFIG ----------------
# Use environment variables for sensitive data if possible
FOLDER_NODE_ID = 2209221
LOCAL_FILE = "/tmp/latest_ot_data.csv" # Use /tmp for Cloud Foundry write access

OPENTEXT_CONFIG = {
    "protocol": "https",
    "hostname": "otcs.g080-test.opentext.cloud",
    "port": "443",
    "username": "WinslowSAP",
    "password": "2lw37Hn63BFe8h0Je19o8TiqsCMpGdZE",
    "base_path": "/cs/cs.exe",
    "node_id": 2594322,
    "local_filename": LOCAL_FILE
}

# ---------------- OPEN TEXT ----------------
def connect_to_opentext():
    otcs = OTCS(
        protocol=OPENTEXT_CONFIG["protocol"],
        hostname=OPENTEXT_CONFIG["hostname"],
        port=OPENTEXT_CONFIG["port"],
        public_url=OPENTEXT_CONFIG["hostname"],
        username=OPENTEXT_CONFIG["username"],
        password=OPENTEXT_CONFIG["password"],
        base_path=OPENTEXT_CONFIG["base_path"]
    )
    otcs.authenticate()
    return otcs

def download_latest_csv(otcs, folder_node_id):
    otcs.download_document(folder_node_id, LOCAL_FILE)

# ---------------- HANA CONNECTION ----------------
def get_connection():
    return dbapi.connect(
        address="62a99a55-f1da-4c0e-ac00-6a37365afe85.hana.prod-ap21.hanacloud.ondemand.com",
        port=443,
        user="DBADMIN",
        password="Pavan@123"
    )

# ---------------- INSERT CSV INTO DB ----------------
def insert_csv_into_db():
    df = pd.read_csv(LOCAL_FILE)
    conn = get_connection()
    cursor = conn.cursor()

    # Get existing NODE_IDs
    cursor.execute("SELECT NODE_ID FROM OPENTEXT_DATA")
    existing_node_ids = {row[0] for row in cursor.fetchall()}

    # Filter only new rows
    data_to_insert = [
        (
            int(row.get("TargetFolderNodeID", 0)),
            row.get("TargetFolderName", ""),
            row.get("Project Name", ""),
            row.get("Type", ""),
            row.get("Project Created Date", "")
        )
        for _, row in df.iterrows()
        if int(row.get("TargetFolderNodeID", 0)) not in existing_node_ids
    ]

    count = 0
    if data_to_insert:
        insert_query = """
            INSERT INTO OPENTEXT_DATA (NODE_ID, FOLDERS, NAME, TYPE, CREATED_DATE)
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        count = len(data_to_insert)
    
    conn.close()
    return count

# ---------------- SERVICE ENDPOINTS ----------------

@app.route("/health", methods=["GET"])
def health():
    """Simple endpoint to check if the service is running."""
    return jsonify({"status": "UP"}), 200

@app.route("/api/v1/sync", methods=["POST"])
def import_csv():
    """
    Main service endpoint. 
    Call this from Postman via POST.
    """
    try:
        logger.info("Starting OpenText to HANA synchronization...")
        
        # 1. Connect and Download
        otcs = connect_to_opentext()
        download_latest_csv(otcs, FOLDER_NODE_ID)
        
        # 2. Process and Insert
        rows_inserted = insert_csv_into_db()
        
        # 3. Return JSON Response
        return jsonify({
            "status": "success",
            "message": "Data synchronization completed",
            "details": {
                "new_rows_inserted": rows_inserted,
                "file_processed": LOCAL_FILE
            }
        }), 200

    except Exception as e:
        logger.error(f"Service Error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ---------------- RUN APP ----------------
if __name__ == "__main__":
    # Get port from BTP environment or default to 8080
    port = int(os.environ.get("PORT", 8080))
    # host 0.0.0.0 is required for Cloud Foundry
    app.run(host="0.0.0.0", port=port)