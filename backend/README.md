# Backend — Train Tracking Algeria

## Run locally
```bash
cd backend
./run.sh
# API: http://127.0.0.1:8000
# Docs: http://127.0.0.1:8000/docs
```

## Pipeline
```
POST /observations
  → validation
  → aggregation
  → confidence + freshness
  → station detection (if trip stops registered)
  → ETA
  → wait decision
  → public aggregate (GET /trips/{id}/live)
```

## Truth rules
- No aggregate → `GET .../live` returns `null` (UNKNOWN)
- Raw GPS observations are never listed on public endpoints
- Reference stations are NOT live train positions

## Register trip stops (for ETA / station detection)
```bash
curl -X POST http://127.0.0.1:8000/admin/trip-stops \
  -H 'Content-Type: application/json' \
  -d '{"trip_id":"trip-demo","station_ids":["st-aga","st-hdey","st-elhar","st-birtouta","st-zeralda"]}'
```
