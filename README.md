# Raising Daisies – Auto-Discovery Worker

Finds new sources automatically, then ingests events into your backend.

## Env Vars
- BACKEND_API_URL (https://...up.railway.app)
- ADMIN_TOKEN (same as backend)
- CITY_LIST (e.g. "Phoenix, Tucson")
- REGION_CODE (e.g. "AZ")
- GCS_API_KEY and GCS_CX (for Google Custom Search)
- EVENTBRITE_TOKEN (optional)

## Cron
python discovery_worker.py
