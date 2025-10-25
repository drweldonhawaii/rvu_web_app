from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import csv, itertools, os, re
import getConvertLatestPracPTPEdits as CONVERTAH

APP_PASSWORD = os.environ.get("APP_PASSWORD", "demo123")
SECRET_KEY = os.environ.get("SECRET_KEY", "change-this-secret")

app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY
VERSION = ""
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "rvus.csv")
CCI_PATH = os.path.join(os.path.dirname(__file__), "data", "cci.csv")

# --- NCCI (f1+f2) auto-update on launch ---
# Base "f1" license URL for the current or last-known quarter; the updater
# will probe revisions, versions, and the next quarter automatically.
BASE_NCCI_F1_URL = os.environ.get("BASE_NCCI_F1_URL",
    "https://www.cms.gov/license/ama?file=/files/zip/medicare-ncci-2025q4-practitioner-ptp-edits-ccipra-v313r0-f1.zip"
)

# Download (or no-op) and point CCI_PATH to the combined CSV it produces.
_ncci_out_csv, _ncci_status = CONVERTAH.download_or_skip_both_with_version(BASE_NCCI_F1_URL,
    out_dir=os.path.join(os.path.dirname(__file__), "data")
)
print("[NCCI]", _ncci_status)
CCI_PATH = str(_ncci_out_csv)


def load_rvu_table():
    table = {}
    if not os.path.exists(DATA_PATH):
        return table
    with open(DATA_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("code", "").strip()
            try:
                rvu = float(row.get("work_rvu") or 0)
            except:
                rvu = 0.0
            if code:
                table[code] = rvu
    return table

def load_cci_table():
    table = {}
    if not os.path.exists(CCI_PATH):
        return table
    with open(CCI_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            # normalize keys to lowercase and strip spaces/asterisks
            row = {re.sub(r"[^a-z0-9]+", "", k.lower()): (v or "").strip()
                   for k, v in raw.items()}
            # try both "column1"/"column2" and "column" + digits
            c1 = row.get("column1") or row.get("col1") or row.get("c1") or row.get("column") or ""
            c2 = row.get("column2") or row.get("col2") or row.get("c2") or ""
            mod = row.get("modifier") or row.get("mod") or ""
            if c1 and c2:
                table[(c1, c2)] = mod
                table[(c2, c1)] = mod  # bidirectional
    return table
    with open(CCI_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            c1 = row.get("column1", "").strip()
            c2 = row.get("column2", "").strip()
            mod = row.get("modifier", "").strip()
            if c1 and c2:
                table[(c1, c2)] = mod
                table[(c2, c1)] = mod  # bidirectional
    return table

RVU_TABLE = load_rvu_table()
CCI_TABLE = load_cci_table()
print("Loaded", len(RVU_TABLE), "codes and", len(CCI_TABLE), "CCI pairs")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == APP_PASSWORD:
            session["authed"] = True
            return redirect(url_for("home"))
        flash("Incorrect password.", "error")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/", methods=["GET", "POST"])
def home():
    if not session.get("authed"):
        return redirect(url_for("login"))
    results = []
    input_codes = ""
    if request.method == "POST":
        input_codes = request.form.get("codes", "")
        codes = [c.strip() for c in input_codes.split(",") if c.strip()]
        combos = []
        n = len(codes)
        for r in range(1, n+1):
            for subset in itertools.combinations(codes, r):
                total = sum(RVU_TABLE.get(code, 0.0) for code in subset)
                valid = True
                notes = []
                for i in range(len(subset)):
                    for j in range(i+1, len(subset)):
                        c1, c2 = subset[i], subset[j]
                        if (c1, c2) in CCI_TABLE:
                            if CCI_TABLE[(c1, c2)] == "0":
                                valid = False
                            elif CCI_TABLE[(c1, c2)] == "1" or CCI_TABLE[(c1, c2)] == "9":
                                notes.append(f"{c1}+{c2} requires modifier {CCI_TABLE[(c1, c2)]}")
                if valid:
                    combos.append({"codes": list(subset), "total": round(total, 2), "notes": notes})
        results = sorted(combos, key=lambda x: x["total"], reverse=True)
    return render_template("index.html", results=results, input_codes=input_codes, has_data=len(RVU_TABLE) > 0)

@app.route("/breakdown", methods=["POST"])
def breakdown():
    if not session.get("authed"):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    codes = data.get("codes", [])
    breakdown = [{"code": c, "rvu": RVU_TABLE.get(c, 0.0)} for c in codes]
    return jsonify({"breakdown": breakdown, "total": sum(item["rvu"] for item in breakdown)})

@app.route("/update", methods=["GET", "POST"])
def update():
    if not session.get("authed"):
        return redirect(url_for("login"))
    if request.method == "POST":
        file = request.files.get("file")
        if not file:
            flash("No file selected.", "error")
            return redirect(url_for("update"))
        save_path = DATA_PATH
        file.save(save_path)
        global RVU_TABLE
        RVU_TABLE = load_rvu_table()
        flash("RVU database updated successfully.", "success")
        return redirect(url_for("home"))
    return render_template("update.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
