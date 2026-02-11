import json
import time
import ssl
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# ================= MQTT CONFIG =================
MQTT_SERVER = "watersupply-scada.gujarat.gov.in"
MQTT_PORT = 8883
MQTT_USERNAME = "CEPL"
MQTT_PASSWORD = "P0144@Eh"
CA_CERT_PATH = "CA.crt"

# ================= FILE PATHS =================
DATA_FILE = "decord_result.jsonl"
TOPIC_MAP_FILE = "device_topic_map.json"
MQTT_LOG_FILE = "mqtt_logs.jsonl"
POSITION_FILE = "last_sent_position.txt"

CHECK_INTERVAL = 5  # seconds

# ================= GLOBAL TRACKER =================
pending_messages = {}

# ================= HELPER FUNCTIONS =================
def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_mqtt_log(entry):
    with open(MQTT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def get_last_position():
    if not os.path.exists(POSITION_FILE):
        return 0
    try:
        with open(POSITION_FILE, "r") as f:
            return int(f.read().strip())
    except:
        return 0

def save_last_position(pos):
    with open(POSITION_FILE, "w") as f:
        f.write(str(pos))

# ================= MQTT CALLBACKS =================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ MQTT Connected Successfully")
    else:
        print("❌ MQTT Connection Failed | RC:", rc)

def on_disconnect(client, userdata, rc):
    print("⚠ MQTT Disconnected. Reconnecting...")
    while True:
        try:
            client.reconnect()
            print("✅ MQTT Reconnected")
            break
        except Exception as e:
            print("⏳ Reconnect failed, retrying...", e)
            time.sleep(5)

def on_publish(client, userdata, mid):
    if mid in pending_messages:
        info = pending_messages.pop(mid)

        print(
            f"✅ UPLOADED | IMEI: {info['imei']} | "
            f"Time: {info['upload_time']} | Topic: {info['topic']}"
        )

        save_mqtt_log({
            "line": info["line"],
            "imei": info["imei"],
            "topic": info["topic"],
            "upload_time": info["upload_time"],
            "status": "SUCCESS",
            "broker_reply": "PUBACK RECEIVED",
            "payload": info["payload"]
        })

        save_last_position(info["line"])

# ================= MQTT SETUP =================
client = mqtt.Client(clean_session=True)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

client.tls_set(
    ca_certs=CA_CERT_PATH,
    cert_reqs=ssl.CERT_NONE,
    tls_version=ssl.PROTOCOL_TLS
)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish

client.connect(MQTT_SERVER, MQTT_PORT, 60)
client.loop_start()

topic_map = load_json(TOPIC_MAP_FILE, {})

print("🚀 Continuous MQTT Sender Started...")

# ================= MAIN LOOP =================
while True:

    if not os.path.exists(DATA_FILE):
        print("❌ Data file not found:", DATA_FILE)
        time.sleep(CHECK_INTERVAL)
        continue

    last_position = get_last_position()

    with open(DATA_FILE, "r", encoding="utf-8") as file:
        for line_no, line in enumerate(file, start=1):

            if line_no <= last_position:
                continue

            try:
                record = json.loads(line.strip())

                imei = record["imei"]
                measure = record.get("decoded_measurements", {})

                actual_flow = float(measure.get("transient_flow", 0))
                total_flow = round(
                    float(measure.get("total_cumulative_whole", 0)) +
                    float(measure.get("total_cumulative_decimal", 0)),
                    3
                )

                date_epoch = int(
                    datetime.strptime(
                        record["timestamp"], "%Y-%m-%d %H:%M:%S"
                    ).replace(hour=0, minute=0, second=0).timestamp()
                )

                mqtt_payload = {
                    "version": "1.0",
                    "onlinetag": imei,
                    "time": date_epoch,
                    "payload": [{
                        "subDeviceId": "ttyCOM1_2",
                        "deviceType": "modbus_2",
                        "status": {
                            "ActualFlow": actual_flow,
                            "TotalFlow": total_flow,
                            "InsDiagnostic": "00000000",
                            "timestamp": int(time.time())
                        }
                    }]
                }

                topic = topic_map.get(imei, imei)
                upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                result = client.publish(
                    topic,
                    json.dumps(mqtt_payload),
                    qos=1
                )

                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    pending_messages[result.mid] = {
                        "line": line_no,
                        "imei": imei,
                        "topic": topic,
                        "upload_time": upload_time,
                        "payload": mqtt_payload
                    }
                else:
                    print(f"❌ PUBLISH FAILED | IMEI: {imei}")

                    save_mqtt_log({
                        "line": line_no,
                        "imei": imei,
                        "topic": topic,
                        "upload_time": upload_time,
                        "status": "FAILED",
                        "error": "Publish return code error",
                        "payload": mqtt_payload
                    })

            except Exception as e:
                print(f"❌ ERROR Line {line_no}: {e}")

                save_mqtt_log({
                    "line": line_no,
                    "status": "ERROR",
                    "error": str(e),
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

    time.sleep(CHECK_INTERVAL)
