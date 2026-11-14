
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_DIR / "data" / "sessions_0.json"
OUTPUT_FILE = PROJECT_DIR / "outputs" / "hbase_sessions_load.hbase"

def ruby_quote(value):
    """Safely quote a value for an HBase shell command."""
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "\\'")
    value = value.replace("\r", "\\r")
    value = value.replace("\n", "\\n")
    return "'" + value + "'"

with open(INPUT_FILE, encoding="utf-8") as file:
    sessions = json.load(file)

commands = []

for session in sessions:
    user_id = session["user_id"]
    session_id = session["session_id"]

    timestamp = datetime.fromisoformat(
        session["start_time"]
    ).replace(tzinfo=timezone.utc)

    epoch_ms = int(timestamp.timestamp() * 1000)
    reverse_timestamp = 9999999999999 - epoch_ms

    row_key = (
        f"{user_id}#{reverse_timestamp:013d}#{session_id}"
    )

    values = {
        "session:session_id": session_id,
        "session:start_time": session["start_time"],
        "session:end_time": session["end_time"],
        "session:duration_seconds": session["duration_seconds"],

        "activity:viewed_products": session["viewed_products"],
        "activity:page_views": session["page_views"],
        "activity:cart_contents": session["cart_contents"],

        "device:type": session.get("device_profile", {}).get("type", ""),
        "device:os": session.get("device_profile", {}).get("os", ""),
        "device:browser": session.get("device_profile", {}).get("browser", ""),

        "location:city": session.get("geo_data", {}).get("city", ""),
        "location:state": session.get("geo_data", {}).get("state", ""),
        "location:country": session.get("geo_data", {}).get("country", ""),
        "location:ip_address": session.get("geo_data", {}).get("ip_address", ""),

        "conversion:status": session["conversion_status"],
        "conversion:referrer": session.get("referrer", ""),
    }

    for column, value in values.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        commands.append(
            f"put 'customer_sessions', {ruby_quote(row_key)}, "
            f"{ruby_quote(column)}, {ruby_quote(value)}"
        )

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE.write_text("\n".join(commands) + "\n", encoding="utf-8")

print("Sessions prepared:", len(sessions))
print("HBase put commands:", len(commands))
print("Output file:", OUTPUT_FILE)
