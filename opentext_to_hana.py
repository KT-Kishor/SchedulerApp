from flask import Flask, render_template_string
import pandas as pd
from pyxecm import OTCS
from hdbcli import dbapi

app = Flask(__name__)

# ---------------- CONFIG ----------------
FOLDER_NODE_ID = 2209221
LOCAL_FILE = "./latest_ot_data.csv"

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

    if data_to_insert:
        insert_query = """
            INSERT INTO OPENTEXT_DATA (NODE_ID, FOLDERS, NAME, TYPE, CREATED_DATE)
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
    conn.close()
    return len(data_to_insert)

# ---------------- FLASK ROUTES ----------------
@app.route("/")
def home():
    # Simple HTML with one button
    html = """
    <h2>OpenText CSV Import</h2>
    <form action="/import" method="post">
        <button type="submit">Import CSV into Database</button>
    </form>
    """
    return render_template_string(html)

# @app.route("/import", methods=["POST"])
# def import_csv():
#     try:
#         otcs = connect_to_opentext()
#         download_latest_csv(otcs, FOLDER_NODE_ID)
#         rows_inserted = insert_csv_into_db()
#         return f"✅ Process completed! {rows_inserted} new rows inserted."
#     except Exception as e:
#         return f"❌ Error: {e}"

# # ---------------- RUN APP ----------------
# if __name__ == "__main__":
#     app.run(debug=True)

@app.route("/import", methods=["POST"])
def import_csv():
    try:
        otcs = connect_to_opentext()
        download_latest_csv(otcs, FOLDER_NODE_ID)
        rows_inserted = insert_csv_into_db()
        return f"✅ Process completed! {rows_inserted} new rows inserted."
    except Exception as e:
        return f"❌ Error: {e}"

# ---------------- RUN APP ----------------
import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

