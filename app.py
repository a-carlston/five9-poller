"""Five9 Agent State Poller.

Polls Five9 Supervisor SOAP API every 60 seconds for agent state snapshots.
Writes each snapshot to Supabase for WFM interval analysis.

Deployed on Render as a web service with a /poll endpoint
triggered by an external cron (cron-job.org) or Render cron job.
"""

import os
import time
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify

app = Flask(__name__)

# ── Config from environment ──────────────────────────────────
FIVE9_USER = os.environ.get("FIVE9_USER", "")
FIVE9_PASS = os.environ.get("FIVE9_PASS", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")  # service role key for writes

FIVE9_SOAP_URL = "https://api.five9.com/wssupervisor/v14/SupervisorWebService"
FIVE9_NS = "http://service.supervisor.ws.five9.com/"

# Five9 domain timezone is Pacific (UTC-8 PST / UTC-7 PDT)
FIVE9_TZ_OFFSET_HOURS = -8

# ── Five9 SOAP helpers ───────────────────────────────────────

def five9_soap_call(method, body_xml=""):
    """Make a SOAP call to Five9 Supervisor API."""
    soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ser="{FIVE9_NS}">
  <soapenv:Header/>
  <soapenv:Body>
    <ser:{method}>{body_xml}</ser:{method}>
  </soapenv:Body>
</soapenv:Envelope>"""

    resp = requests.post(
        FIVE9_SOAP_URL,
        auth=(FIVE9_USER, FIVE9_PASS),
        headers={"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": ""},
        data=soap,
        timeout=30,
    )
    return resp


def five9_set_session():
    """Set session parameters (required before pulling stats)."""
    body = """<viewSettings>
        <rollingPeriod>Today</rollingPeriod>
        <statisticsRange>CurrentDay</statisticsRange>
      </viewSettings>"""
    resp = five9_soap_call("setSessionParameters", body)
    return resp.status_code == 200


def five9_get_agent_states():
    """Pull AgentState statistics and parse XML into list of dicts."""
    import xml.etree.ElementTree as ET

    resp = five9_soap_call("getStatistics",
                           "<statisticType>AgentState</statisticType>")
    if resp.status_code != 200:
        return None, f"Five9 returned {resp.status_code}: {resp.text[:500]}"

    root = ET.fromstring(resp.text)
    # Strip namespaces for easier parsing
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    # Get column names
    columns_node = root.find(".//columns/values")
    if columns_node is None:
        return None, "No columns found in response"
    columns = [d.text or "" for d in columns_node.findall("data")]

    # Get rows
    agents = []
    for row_node in root.findall(".//rows"):
        values_node = row_node.find("values")
        if values_node is None:
            continue
        values = [d.text or "" for d in values_node.findall("data")]
        agent = dict(zip(columns, values))
        agents.append(agent)

    return agents, None


# ── Supabase writer ──────────────────────────────────────────

def write_to_supabase(agents, snapshot_ts):
    """Write agent snapshot rows to Supabase."""
    rows = []
    for a in agents:
        # Skip logged-out agents
        if a.get("State") == "Logged Out":
            continue
        # Convert Five9 Pacific timestamp to UTC
        state_since_raw = a.get("State Since", "")
        state_since_utc = ""
        if state_since_raw:
            try:
                naive = datetime.strptime(state_since_raw, "%Y-%m-%d %H:%M:%S")
                utc_dt = naive - timedelta(hours=FIVE9_TZ_OFFSET_HOURS)
                state_since_utc = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                state_since_utc = state_since_raw

        rows.append({
            "snapshot_ts": snapshot_ts,
            "username": a.get("Username", ""),
            "full_name": a.get("Full Name", ""),
            "state": a.get("State", ""),
            "reason_code": a.get("Reason Code", ""),
            "state_since": state_since_raw,
            "state_since_utc": state_since_utc,
            "state_duration": a.get("State Duration", ""),
            "campaign_name": a.get("Campaign Name", ""),
            "call_type": a.get("Call Type", ""),
            "media_availability": a.get("Media Availability", ""),
        })

    if not rows:
        return 0, None

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/five9_agent_snapshots",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=rows,
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        return 0, f"Supabase error {resp.status_code}: {resp.text[:500]}"

    return len(rows), None


# ── Flask endpoints ──────────────────────────────────────────

@app.route("/")
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "five9-poller"})


@app.route("/poll")
def poll():
    """Grab Five9 agent state snapshot and write to Supabase."""
    start = time.time()
    snapshot_ts = datetime.now(timezone.utc).isoformat()

    # Set session params (required each call since no persistent session)
    if not five9_set_session():
        return jsonify({"ok": False, "error": "Failed to set Five9 session"}), 500

    # Pull agent states
    agents, err = five9_get_agent_states()
    if err:
        return jsonify({"ok": False, "error": err}), 500

    # Write to Supabase
    count, err = write_to_supabase(agents, snapshot_ts)
    if err:
        return jsonify({"ok": False, "error": err}), 500

    # Purge snapshots older than 48 hours
    purged = 0
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    del_resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/five9_agent_snapshots?snapshot_ts=lt.{cutoff}",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "return=representation",
        },
        timeout=30,
    )
    if del_resp.status_code == 200:
        purged = len(del_resp.json())

    elapsed = round(time.time() - start, 2)
    return jsonify({
        "ok": True,
        "agents_total": len(agents),
        "agents_written": count,
        "purged": purged,
        "snapshot_ts": snapshot_ts,
        "elapsed_sec": elapsed,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
