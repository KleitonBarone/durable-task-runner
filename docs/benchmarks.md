# Database measurements

Run these benchmarks against an isolated PostgreSQL container:

```sh
go run ./cmd/dev bench '-bench=BenchmarkClaim|BenchmarkTimeline' '-benchtime=100x' .
go run ./cmd/dev bench '-bench=BenchmarkRecoveryBatch100' '-benchtime=3x' .
```

`bench` provisions and removes its own database. It defaults to three repetitions
and three iterations per benchmark, avoiding unbounded fixture setup during
automatic calibration. Increase iterations explicitly for a longer measurement.
Setup, migrations, and fixture insertion are outside the timed region. Claims
use the production PostgreSQL time authority and leave leases active until fixture
cleanup. Recovery injects expiration before timing instead of waiting for leases.

Measured on 2026-10-17 with Go 1.26.9, Windows amd64, a Ryzen 7 5700X3D with eight
cores and sixteen logical processors, and PostgreSQL 17 in Docker through WSL
Ubuntu. The database used temporary memory-backed storage, an ephemeral loopback
port, default server settings, and the test helper's pool. Client/server latency
includes the Windows/WSL boundary. These are warm local measurements, not a
production capacity promise or durable-storage benchmark.

| Workload | Three measurements |
| --- | --- |
| Serial claim, 100 independent ready activities | 3.78 / 4.07 / 4.58 ms per claim |
| Four concurrent claimers, 100 activities total | 1.71 / 1.64 / 1.53 ms elapsed per claim |
| Empty polls in contention run | 0.09 / 0.12 / 0.10 per successful claim |
| Recover 100 expired activities per batch, three batches | 400 / 415 / 551 ms per batch |
| First timeline page of 100 rows, 100 added events | 2.51 / 2.00 / 2.01 ms per page |
| First timeline page of 100 rows, 10,000 added events | 2.06 / 2.12 / 1.90 ms per page |

The contention figure divides total elapsed time by successful claims; it is not
individual request latency. The timeline runs measure a bounded first page, not a
full export. They support keeping bounded pages and the existing history index.
They do not justify adding infrastructure or claiming linear scaling.

## Bounded recovery load

```sh
go run ./cmd/dev e2e '-tags=integration' '-run=TestBoundedLoad' .
```

This deterministic manual-clock test starts 200 two-step jobs, injects 20 first
step effects without acknowledgement, expires their leases, and runs four workers
with two briefly overlapping scheduler calls. It asserts 200 completed workflows,
400 completed activities, 420 attempts including 20 abandoned ones, 220 first-step
deliveries, 200 deduplicated effects, and zero active leases. It also reads typed
results and scans all workflows in bounded pages. The local run passed in 2.54
seconds. This tests correctness under bounded contention, not elapsed-time capacity.
