from datetime import datetime
import pytz

utc = pytz.UTC
et = pytz.timezone("America/New_York")

now_utc = datetime.now(utc)
now_et = now_utc.astimezone(et)

print("TZ PROBE")
print("UTC:", now_utc.isoformat())
print("ET :", now_et.isoformat())
