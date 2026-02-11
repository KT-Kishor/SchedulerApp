from flask import Flask, request, jsonify
from hdbcli import dbapi

app = Flask(__name__)

# HANA connection function
def get_connection():
    return dbapi.connect(
        address="62a99a55-f1da-4c0e-ac00-6a37365afe85.hana.prod-ap21.hanacloud.ondemand.com",
        port=443,
        user="DBADMIN",
        password="Pavan@123"
    )

@app.route("/")
def home():
    return "Hello from Python App in SAP BAS"

# Test DB connection
@app.route("/testdb")
def test_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP FROM DUMMY")
    result = cursor.fetchone()
    conn.close()
    return str(result)


# @app.route("/createtable")
# def create_table():
#     conn = get_connection()
#     cursor = conn.cursor()
#     cursor.execute("""
#         CREATE TABLE EMPLOYEE (
#             ID INTEGER PRIMARY KEY,
#             NAME NVARCHAR(100),
#             AGE INTEGER
#         )
#     """)
#     conn.commit()
#     conn.close()
#     return "Table created successfully"

@app.route("/openText")
def create_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE OPENTEXT_DATA (
        NODE_ID INTEGER,
        FOLDERS NVARCHAR(200),
        NAME NVARCHAR(200),
        TYPE NVARCHAR(50),
        CREATED_DATE NVARCHAR(50)
    )
    """)
    conn.commit()
    conn.close()
    return "Table created successfully"

# CREATE
@app.route("/employee", methods=["POST"])
def create_employee():
    data = request.json
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO EMPLOYEE (ID, NAME, AGE) VALUES (?, ?, ?)",
        (data["id"], data["name"], data["age"])
    )
    conn.commit()
    conn.close()
    return "Employee inserted"

# READ
@app.route("/employee", methods=["GET"])
def get_employees():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM EMPLOYEE")
    rows = cursor.fetchall()
    conn.close()
    return jsonify(rows)

# UPDATE
@app.route("/employee/<int:id>", methods=["PUT"])
def update_employee(id):
    data = request.json
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE EMPLOYEE SET NAME=?, AGE=? WHERE ID=?",
        (data["name"], data["age"], id)
    )
    conn.commit()
    conn.close()
    return "Employee updated"

# DELETE
@app.route("/employee/<int:id>", methods=["DELETE"])
def delete_employee(id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM EMPLOYEE WHERE ID=?", (id,))
    conn.commit()
    conn.close()
    return "Employee deleted"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
