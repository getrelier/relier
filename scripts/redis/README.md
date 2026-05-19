# Relier Redis — High Availability

Relier's coordination state (Phoenix resurrection, idempotency, admission
control, locks, SLO telemetry) lives entirely in Redis. This directory makes
that backbone survive the loss of any single node.

## Which stack runs what

| Stack                     | Redis layout                          | Sentinel |
|---------------------------|---------------------------------------|----------|
| `docker-compose.yml` (dev)| Single node, AOF + RDB persistence    | Off      |
| `docker-compose.prod.yml` | Master + 2 replicas + 3 Sentinels     | On       |

Dev stays single-node to keep local startup fast — it still has full
persistence, just no failover. To exercise the HA / failover path locally,
run the production manifest (it works fine on a laptop):

```sh
REDIS_PASSWORD=dev SENTINEL_PASSWORD=dev docker compose -f docker-compose.prod.yml up
```

Then `docker kill relier-redis-master` and watch a Sentinel promote a replica.

## Topology (production manifest)

| Role     | Service(s)                                        | Purpose                          |
|----------|---------------------------------------------------|----------------------------------|
| Master   | `relier-redis-master`                             | Accepts all reads + writes       |
| Replica  | `relier-redis-replica-1`, `relier-redis-replica-2`| Sync copies; failover candidates |
| Sentinel | `relier-sentinel-1/2/3`                           | Monitor master, elect new master |
| Backup   | `relier-redis-backup`                             | Periodic RDB snapshot + rotation |

Sentinel quorum is **2 of 3** — a failover proceeds only when two Sentinels
agree the master is unreachable, which prevents a single partitioned Sentinel
from causing split-brain.

## Persistence

Both AOF and RDB are enabled (`redis.conf`):

- **AOF** (`appendfsync everysec`) — the durability mechanism. At most one
  second of acknowledged writes is at risk on a hard crash.
- **RDB** (`save` rules) — a compact point-in-time snapshot used for fast
  restarts and as the artifact the backup sidecar archives.

## Backups

`backup.sh` runs in the `relier-redis-backup` sidecar. Each pass:

1. Issues `BGSAVE` against a **replica** (master write path stays unloaded).
2. Waits for `LASTSAVE` to advance, then gzips `dump.rdb` to the `redis_backups` volume.
3. Rotates archives, keeping the newest `BACKUP_RETENTION` (default 168 ≈ 7 days hourly).
4. If `BACKUP_S3_BUCKET` is set (and the image has the `aws` CLI), uploads an offsite copy.

### Restore

1. Stop the stack: `docker compose -f docker-compose.prod.yml down`.
2. Extract a chosen archive over the master's data volume (the `relier-prod_`
   prefix comes from the `name:` in `docker-compose.prod.yml`):
   ```sh
   docker run --rm -v relier-prod_redis_master_data:/data \
     -v relier-prod_redis_backups:/backups alpine \
     sh -c "gunzip -c /backups/relier-redis-<timestamp>.rdb.gz > /data/dump.rdb"
   ```
3. Remove the stale AOF so Redis loads the restored RDB:
   `appendonly.aof*` (or `appendonlydir/`) in the same volume.
4. Start the stack — replicas resync from the master automatically.

## Enabling Sentinel in the application

Sentinel wiring is config-gated and **off by default** (`redis_use_sentinel`),
so the dev stack and the integration test suite are unaffected.
`docker-compose.prod.yml` sets the relevant `RELIER_*` env vars to turn it on:

- `RELIER_REDIS_USE_SENTINEL=true`
- `RELIER_REDIS_SENTINEL_NODES=relier-sentinel-1:26379,relier-sentinel-2:26379,relier-sentinel-3:26379`
- `RELIER_REDIS_SENTINEL_MASTER_NAME=relier-master`
