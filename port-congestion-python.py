import os, calendar, datetime, requests, pandas as pd, json
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

API_URL   = "https://api.spire.com/graphql"
API_TOKEN =  os.environ["PORT_CONGESTION_API_TOKEN"]

START_DATE = datetime.date(2025, 1, 1)
END_DATE   = datetime.date(2025, 5, 20)               # inclusive

UNLOCODES = [
    "USEWR","USSAV","USPEF","USCHS","USOAK","USLAX","USHOU","USLGB","USMIA","USORF",
]

#"USPTM","USPHL","USJAX","USTIW","USMSY","USILM","USSEA","USBAL","USMOB","USHNL"

GRAPHQL = """
query DailyCongestion($port: PortCongestionPortInput!, $start: Date!, $end: Date!) {
  portCongestion(
    port:     $port
    vessels:  { shipType: [CONTAINER] }
    dateRange:{ startDate: $start, endDate: $end }
  ) {
    congestionIndex {
      byTimeInterval {
        timeInterval { endTime }
        value        { index level }
      }
    }
  }
}
"""

def month_chunks(start: datetime.date, end: datetime.date):
    cur = start
    while cur <= end:
        last_day  = calendar.monthrange(cur.year, cur.month)[1]
        chunk_end = datetime.date(cur.year, cur.month, last_day)
        if chunk_end > end:
            chunk_end = end
        yield cur, chunk_end
        cur = chunk_end + datetime.timedelta(days=1)

# Robust session with retries
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=1.5, status_forcelist=[502,503,504], allowed_methods=["POST"]
)))

rows = []

for unloc in UNLOCODES:
    print(f"\n### {unloc} ###")
    for c_start, c_end in month_chunks(START_DATE, END_DATE):
        payload = {
            "query": GRAPHQL,
            "variables": {
                "port":  {"unlocode": unloc},
                "start": c_start.isoformat(),
                "end":   c_end.isoformat()
            }
        }
        r = sess.post(
            API_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {API_TOKEN}",
                "User-Agent": "multi-port-congestion/1.1",
                "Connection": "close"
            },
            timeout=30
        )

        # --- graceful error reporting --------------------------------------
        if r.status_code != 200:
            print(f"HTTP {r.status_code} — {r.text[:200]}…")
            r.raise_for_status()

        resp = r.json()
        if "errors" in resp:
            print(json.dumps(resp["errors"], indent=2))
            raise RuntimeError("GraphQL returned errors")

        data = resp["data"]["portCongestion"]["congestionIndex"]["byTimeInterval"]
        rows.extend({
            "port":  unloc,
            "date":  itm["timeInterval"]["endTime"][:10],
            "index": itm["value"]["index"],
            "level": itm["value"]["level"]
        } for itm in data)

        print(f"✓ {unloc} {c_start:%Y-%m} ({len(data)} days)")

df = pd.DataFrame(rows).sort_values(["port", "date"])
print("\nTotal rows:", len(df))
print(df.head())

df.to_csv("congestion_daily_20ports.csv", index=False)
