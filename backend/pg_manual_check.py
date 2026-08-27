"""End-to-end test of the PostgresStore adapter against a live Postgres."""
import time

from app.main import app  # noqa: E402 — triggers PostgresStore boot
from fastapi.testclient import TestClient

c = TestClient(app)

# 1) session
s = c.post('/monitor-sessions', json={
    'trip_id': 'trip-pg-1', 'train_id': 'train-pg-1'}).json()
print('session:', s['id'][:8], s['status'])

# 2) trip stops (admin helper)
r = c.post('/admin/trip-stops', json={
    'trip_id': 'trip-pg-1',
    'station_ids': ['st-aga', 'st-caroubier', 'st-elhar', 'st-gue']})
print('stops:', r.json())

# 3) observations
ts = int(time.time() * 1000)
obs = []
for i, (lat, lon) in enumerate([
        (36.765, 3.058), (36.752, 3.068), (36.744, 3.086)]):
    obs.append({'session_id': s['id'], 'trip_id': 'trip-pg-1',
                'train_id': 'train-pg-1', 'latitude': lat, 'longitude': lon,
                'accuracy': 8.0, 'speed': 12.0, 'heading': 90.0,
                'timestamp': ts + i * 60000})
r = c.post('/observations/batch', json=obs)
print('batch:', [o.get('status') for o in r.json()])

# 4) live state
live = c.get('/trips/trip-pg-1/live').json()
assert live is not None, 'live must not be None after observations'
print('live truth:', live['truth'],
      '| next_station:', live.get('next_station', {}).get('name_ar'),
      '| sources:', live.get('source_count'))
assert live['truth'] == 'OBSERVED'

# 5) no raw observations leak in public API
assert 'observations' not in live
print('no raw observation leak: OK')

# 6) trains + nearby
print('trains:', c.get('/trains').json())
print('nearby:', len(c.get('/nearby-trains', params={
      'lat': 36.75, 'lon': 3.07, 'radius': 10000}).json()), 'train(s)')

# 7) stale trip returns None (TTL) — fake old aggregate via direct SQL
from app.store import store
with store._pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.aggregated_train_positions
            SET last_observed_at = now() - interval '2 hours'
            WHERE trip_id = 'trip-pg-1'""")
evicted = store.evict_stale_db()
print('evicted from Postgres:', evicted)
live2 = c.get('/trips/trip-pg-1/live').json()
assert live2 is None, 'stale aggregate must not be published'
print('TTL eviction: OK')

print('ALL POSTGRES ADAPTER TESTS PASSED')
