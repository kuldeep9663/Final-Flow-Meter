from flask import Flask, render_template, jsonify, request, redirect, session, url_for, abort
import json, os
from datetime import datetime, timedelta
from functools import wraps

# ================= CONFIG =================
DATA_FILE = "decord_result.jsonl"
SITES_FILE = "sites.json"
USERS_FILE = "users.json"
ADMIN_FILE = "admin.json"
MQTT_MAP_FILE = "device_topic_map.json"
MQTT_LOG_FILE = "mqtt_logs.jsonl"
DEVICES_FILE = "devices.json"

OFFLINE_THRESHOLD = 900

app = Flask(__name__)
app.secret_key = "aarohi-secure-secret"
app.permanent_session_lifetime = timedelta(minutes=30)

# ================= AUTH DECORATOR =================
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in") or not session.get("username"):
            session.clear()
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

# ================= HELPERS =================
def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(x) for x in f if x.strip()]
def load_json(path):
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except (json.JSONDecodeError, IOError):
        return {}


def group_by_imei(records):
    result = {}
    for r in records:
        result.setdefault(r["imei"], []).append(r)
    for k in result:
        result[k].sort(key=lambda x: x["timestamp"])
    return result
def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_sites():
    return load_json(SITES_FILE)

def save_sites(data):
    with open(SITES_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_users():
    return load_json(USERS_FILE)

def save_users(data):
    with open(USERS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_admin():
    return load_json(ADMIN_FILE)

def all_imeis():
    return list(group_by_imei(load_jsonl(DATA_FILE)).keys())

def available_imeis():
    used = set()
    for s in load_sites().values():
        used.update(s["modems"])
    return [i for i in all_imeis() if i not in used]

# ================= MQTT HELPERS =================
def load_mqtt_map():
    return load_json(MQTT_MAP_FILE)

def save_mqtt_map(data):
    with open(MQTT_MAP_FILE, "w") as f:
        json.dump(data, f, indent=2)

def mqtt_last_sent():
    last = {}
    if not os.path.exists(MQTT_LOG_FILE):
        return last
    with open(MQTT_LOG_FILE, "r") as f:
        for line in f:
            try:
                r = json.loads(line)
                last[r["imei"]] = r["timestamp"]
            except:
                pass
    return last
# ✅ ONLY VALID SITES (FIX)
def valid_sites_only():
    sites = load_sites()
    valid_imeis = set(all_imeis())
    filtered = {}

    for name, site in sites.items():
        modems = [m for m in site.get("modems", []) if m in valid_imeis]
        if modems:
            filtered[name] = {
                "location": site.get("location", ""),
                "modems": modems
            }
    return filtered
def available_imeis():
    used = set()
    for s in load_sites().values():
        used.update(s["modems"])
    return [i for i in all_imeis() if i not in used]
def unassigned_sites_only():
    sites = load_sites()
    users = load_users()

    assigned = set()
    for u in users.values():
        assigned.update(u.get("sites", []))

    return [s for s in sites.keys() if s not in assigned]

# ================= DEVICES =================
def load_devices():
    return load_json(DEVICES_FILE)

def save_devices(data):
    save_json(DEVICES_FILE, data)

def all_imeis():
    devices = set(load_devices().keys())
    mqtt = set(group_by_imei(load_jsonl(DATA_FILE)).keys())
    return sorted(devices | mqtt)
# ================= LOGIN =================
@app.route("/")
def root():
    # 🔹 Direct open → login page
    return redirect(url_for("login"))   


@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""

    if request.method == "POST":
        u = request.form["username"]
        p = request.form["password"]

        admin = load_admin()
        users = load_users()

        # ✅ ADMIN LOGIN → index.html
        if admin and u == admin.get("username") and p == admin.get("password"):
            session.clear()
            session.permanent = True
            session["logged_in"] = True
            session["username"] = u
            session["role"] = "admin"
            return redirect(url_for("admin_dashboard"))

        # ✅ USER LOGIN → user dashboard
        if u in users and users[u]["password"] == p:
            session.clear()
            session.permanent = True
            session["logged_in"] = True
            session["username"] = u
            session["role"] = "user"
            session["sites"] = users[u]["sites"]
            return redirect(url_for("user_dashboard"))

        error = "Invalid username or password"

    return render_template("login.html", error=error)

@app.route("/admin_dashboard")
@login_required
def admin_dashboard():
    if session.get("role") != "admin":
        abort(403)
    return render_template("admin_dashboard.html", user=session["username"])

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ================= API =================
@app.route("/api/modems")
@login_required
def api_modems():
    now = datetime.now()
    result = []

    for imei, logs in group_by_imei(load_jsonl(DATA_FILE)).items():
        last = logs[-1]
        last_time = datetime.strptime(last["timestamp"], "%Y-%m-%d %H:%M:%S")
        online = (now - last_time).total_seconds() <= OFFLINE_THRESHOLD

        result.append({
            "imei": imei,
            "last_seen": last["timestamp"],
            "status": "ONLINE" if online else "OFFLINE"
        })

    return jsonify(result)

@app.route("/api/logs/<imei>")
@login_required
def api_logs(imei):
    return jsonify([r for r in load_jsonl(DATA_FILE) if r["imei"] == imei])
@app.route("/api/available-modems")
@login_required
def api_available_modems():
    if session.get("role") != "admin":
        abort(403)

    return jsonify(available_imeis())

@app.route("/api/sites", methods=["GET", "POST"])
@login_required
def api_sites():
    sites = load_sites()
    if request.method == "POST":
        data = request.json
        sites[data["name"]] = {
            "location": data["location"],
            "modems": data["modems"]
        }
        save_sites(sites)
    return jsonify(sites)

# ================= USER MANAGEMENT API =================
@app.route("/api/users", methods=["GET", "POST"])
@login_required
def api_users():
    users = load_users()
    if request.method == "POST":
        data = request.json
        users[data["username"]] = {
            "password": data["password"],
            "sites": data.get("sites", []),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        save_users(users)
    return jsonify(users)

# ================= MQTT PORTAL API =================
@app.route("/api/mqtt-portal", methods=["GET", "POST"])
@login_required
def api_mqtt_portal():
    if request.method == "GET":
        return jsonify({
            "imeis": all_imeis(),
            "mapping": load_mqtt_map(),
            "last_sent": mqtt_last_sent()
        })
    save_mqtt_map(request.json)
    return jsonify({"status": "saved"})

@app.route("/api/mqtt-logs/<imei>")
@login_required
def api_mqtt_logs(imei):
    logs = []
    if os.path.exists(MQTT_LOG_FILE):
        with open(MQTT_LOG_FILE, "r") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("imei") == imei:
                        logs.append(r)
                except:
                    pass
    return jsonify(logs)

# ================= PAGES =================
@app.route("/")
@login_required
def index():
    if session.get("role") != "admin":
        abort(403)
    return render_template("index.html", user=session["username"])
# ================= REPORT PAGE =================
@app.route("/report")
@login_required
def report():
    if session.get("role") != "admin":
        abort(403)
    return render_template("report.html")

@app.route("/sites")
@login_required
def sites():
    if session.get("role") != "admin":
        abort(403)
    return render_template("sites.html", user=session["username"],sites=valid_sites_only())

@app.route("/site/<name>")
@login_required
def site_view(name):
    if session.get("role") != "admin":
        abort(403)
    sites = valid_sites_only()
    if name not in sites:
        return "Site not found", 404
    return render_template("site_view.html", name=name, site=sites[name])

@app.route("/logs/<imei>")
@login_required
def logs(imei):
    if session.get("role") != "admin":
        abort(403)
    return render_template("logs.html", imei=imei)

@app.route("/mqtt-portal")
@login_required
def mqtt_portal():
    if session.get("role") != "admin":
        abort(403)
    return render_template("mqtt_portal.html")

@app.route("/mqtt-logs/<imei>")
@login_required
def mqtt_logs_page(imei):
    if session.get("role") != "admin":
        abort(403)
    return render_template("mqtt_logs.html", imei=imei)

@app.route("/user_management", methods=["GET", "POST"])
@login_required
def user_management():
    if session.get("role") != "admin":
        abort(403)

    users = load_users()

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        sites = request.form.getlist("sites")

        if not username or not password:
            return redirect(url_for("user_management"))

        users[username] = {
            "password": password,
            "sites": sites,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        save_json(USERS_FILE, users)
        return redirect(url_for("user_management"))

    return render_template(
        "user_management.html",
        user=session["username"],
        sites=unassigned_sites_only(),   # ✅ ONLY FREE SITES
        users=users
    )
# ================= VIEW USER SITES (🔥 MISSING PIECE) =================
@app.route("/user-sites/<username>")
@login_required
def view_user_sites(username):
    if session.get("role") != "admin":
        abort(403)

    users = load_users()
    sites = load_sites()

    if username not in users:
        abort(404)

    now = datetime.now()
    last_map = {
        imei: logs[-1]["timestamp"]
        for imei, logs in group_by_imei(load_jsonl(DATA_FILE)).items()
    }

    user_sites = {}

    for site_name in users[username].get("sites", []):
        site = sites.get(site_name)
        if not site:
            continue

        user_sites[site_name] = []
        for imei in site.get("modems", []):
            last = last_map.get(imei)
            status = "OFFLINE"
            if last:
                t = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
                if (now - t).total_seconds() <= OFFLINE_THRESHOLD:
                    status = "ONLINE"

            user_sites[site_name].append({
                "imei": imei,
                "status": status,
                "last_seen": last
            })

    return render_template(
        "user_site_status.html",
        username=username,
        user_sites=user_sites
    )

# ================= USER DASHBOARD =================
@app.route("/user-dashboard")
@login_required
def user_dashboard():
    if session.get("role") != "user":
        abort(403)

    sites = valid_sites_only()
    now = datetime.now()
    last_map = {}

    for imei, logs in group_by_imei(load_jsonl(DATA_FILE)).items():
        last_map[imei] = logs[-1]["timestamp"]

    dashboard = {}

    for site in session["sites"]:
        if site not in sites:
            continue

        dashboard[site] = []
        for imei in sites[site]["modems"]:
            last = last_map.get(imei)
            status = "OFFLINE"

            if last:
                t = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
                if (now - t).total_seconds() <= OFFLINE_THRESHOLD:
                    status = "ONLINE"

            dashboard[site].append({
                "imei": imei,
                "status": status,
                "last_seen": last
            })

    return render_template("user_dashboard.html", dashboard=dashboard)
@app.route("/user-report")
@login_required
def user_report():
    if session.get("role") != "user":
        abort(403)

    sites = load_sites()
    now = datetime.now()

    # map last seen time
    last_map = {}
    for imei, logs in group_by_imei(load_jsonl(DATA_FILE)).items():
        last_map[imei] = logs[-1]["timestamp"]

    rows = []

    for site in session.get("sites", []):
        site_data = sites.get(site)
        if not site_data:
            continue

        for imei in site_data.get("modems", []):
            last = last_map.get(imei)
            status = "OFFLINE"

            if last:
                t = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
                if (now - t).total_seconds() <= OFFLINE_THRESHOLD:
                    status = "ONLINE"

            rows.append({
                "imei": imei,
                "status": status,
                "last_seen": last
            })

    return render_template("user_report.html", rows=rows)
# ================= ADD NEW DEVICE PAGE =================
@app.route("/add-device", methods=["GET", "POST"])
@login_required
def add_device():
    if session.get("role") != "admin":
        abort(403)

    devices = load_devices()
    error = ""
    success = ""

    if request.method == "POST":
        imei = request.form.get("imei", "").strip()

        if not imei.isdigit() or len(imei) != 15:
            error = "IMEI must be exactly 15 digits"
        elif imei in devices:
            error = "Device already exists"
        else:
            devices[imei] = {
                "added_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            save_devices(devices)
            success = "Device added successfully"

    return render_template(
        "add_new_device.html",
        devices=devices,   # ✅ SEND DEVICES TO PAGE
        error=error,
        success=success
    )

@app.route("/user-logs/<imei>")
@login_required
def user_logs(imei):
    if session.get("role") != "user":
        abort(403)

    allowed = []
    sites = load_sites()
    for s in session["sites"]:
        allowed += sites[s]["modems"]

    if imei not in allowed:
        abort(403)

    logs = [l for l in load_jsonl(DATA_FILE) if l["imei"] == imei]
    return render_template("user_logs.html", imei=imei, logs=logs)

# ================= RUN =================
if __name__ == "__main__":
    print("🚀 Aarohi Dashboard Running")
    app.run(host="0.0.0.0", port=8888, debug=False)

