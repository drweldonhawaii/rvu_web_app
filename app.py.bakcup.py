from flask import Flask, render_template, request, redirect, url_for
import csv
import json
from datetime import datetime

app = Flask(__name__)

# Load configuration
with open("data/data_sources.json") as f:
    CONFIG = json.load(f)

def load_rvu_table():
    rvu_table = {}
    with open(CONFIG["datasets"]["rvus"]["file_path"]) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rvu_table[row["code"]] = float(row["work_rvu"])
            except (ValueError, KeyError):
                continue
    print(f"Loaded {len(rvu_table)} RVU codes")
    return rvu_table

def load_cci_table():
    cci = []
    today = datetime.today().date()
    with open(CONFIG["datasets"]["cci"]["file_path"]) as f:
        reader = csv.DictReader(f)
        for row in reader:
            eff = row.get("effective_date")
            del_date = row.get("deletion_date")
            modifier = row.get("modifier", "")
            if CONFIG["app_settings"]["ignore_expired_edits"]:
                if del_date and del_date < str(today):
                    continue
            cci.append(row)
    print(f"Loaded {len(cci)} active CCI pairs")
    return cci

RVU_TABLE = load_rvu_table()
CCI_TABLE = load_cci_table()

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        codes = request.form["codes"].replace(" ", "").split(",")
        results = []
        total_rvu = 0
        for code in codes:
            rvu = RVU_TABLE.get(code, 0)
            total_rvu += rvu
        results.append({
            "codes": ", ".join(codes),
            "total_rvu": total_rvu
        })
        return render_template("results.html", results=results)
    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
