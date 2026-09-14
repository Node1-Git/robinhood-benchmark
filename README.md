# Robinhood Benchmarks

Reproducible Python tools for comparing Node1 with Robinhood's public feed and direct sequencer submission paths. This repository contains benchmark tools and aggregate findings; credentials, infrastructure configuration, wallet identifiers and historical raw results are excluded.

## Benchmark results

### Feed — Ohio · September 12, 2026

**Node1 delivered matching messages 47.62 ms earlier at the median in Ohio 2b and 59.00 ms earlier in Ohio 2c**, compared with the official Robinhood mainnet Feed.

| Location | Median arrival lead | Node1 first-arrival rate | Matched messages |
| --- | ---: | ---: | ---: |
| Ohio 2b | **47.62 ms** | **99.989%** | 17,514 |
| Ohio 2c | **59.00 ms** | **99.949%** | 17,512 |

Each location completed six five-minute windows, totaling 30 minutes of effective sampling. Both feeds were received simultaneously on the same host and paired by sequence number and verified content. No single-feed-only messages or content mismatches were observed during measurement and matching. Arrival lead is official receipt time minus Node1 receipt time, not absolute sequencer latency.

### Landing — Frankfurt and Ohio · September 14, 2026

**Node1 won 66 of 100 on-chain races from Frankfurt and 61 of 100 from Ohio.**

| Submission path | Frankfurt origin | Ohio origin |
| --- | ---: | ---: |
| Node1 Ohio 2b | 38% | 43% |
| Node1 Ohio 2c | 28% | 18% |
| **Node1 combined — two entry points** | **66%** | **61%** |
| **Direct official sequencer — three entry points** | **34%** | **39%** |

At each location, five paths simultaneously submitted distinct pre-signed candidates sharing sender, nonce and gas settings. Successful on-chain hashes determined winners, not HTTP response times. All 100 rounds per location were retained. Maximum application-level send-start skew was 18.889 µs in Frankfurt and 0.700 µs in Ohio. The two locations were tested in separate sessions.

These findings cover the stated locations and test periods. Landing is a same-nonce competition affected by admission/replacement rules and Node1 internal fan-out; its win rate cannot be converted to a millisecond advantage. Neither test establishes performance across all regions or guarantees future results.

## Setup

Use Linux with Python 3.11+ (tested on Python 3.12). Feed collectors share CPU 0. Landing uses CPUs 0–4 for five senders and CPU 5 for the coordinator, so six available logical CPUs are required. Dedicated cores reduce scheduling noise.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
mkdir -p secrets
chmod 700 secrets
# Edit .env locally with your Feed URL and current official sequencer IPs.
# Save your own Landing UUID in secrets/landing-uuid (mode 600).
set -a
. ./.env
set +a
```

Use your own subscriptions and a dedicated funded test wallet. No DigitalOcean API key is needed: provisioning and internal account selection are intentionally excluded. Private keys are entered through a hidden prompt and are never written by the preparation script. Node1 UUID authentication is sent only to Node1; official HTTPS connections retain certificate and hostname verification.

## Feed arrival benchmark

```sh
python3 feed/benchmark.py
python3 feed/analyze.py ./data/feed
```

Six rounds each use 20 seconds of warm-up, 300 seconds of measurement and 10 seconds of drain: 30 minutes of effective sampling. Independent receivers record monotonic nanosecond timestamps immediately after complete WebSocket message delivery, before parsing; compression is disabled. Both receivers and the coordinator share CPU 0. Existing workloads are not stopped or changed. Unlike the original deployment wrapper, the public script does not require an internal business service to exist.

Messages are paired by sequence number and checked with SHA-256 over canonicalized complete message content. First observations are used for duplicates. Positive `official receipt − Node1 receipt` means Node1 arrived earlier. Integrity reports include unmatched messages, mismatches, gaps, errors and reconnect exclusions. After reconnects, 20 seconds are excluded from paired latency statistics. Analysis includes percentiles, first-arrival rate and 2,000 bootstrap resamples of one-minute blocks. These are relative application receipt times, not absolute sequencer latency.

Completed rounds are checkpointed. Restarting skips completed rounds and preserves interrupted attempts in a separate folder. Use a new output directory for a different experiment; reserve several GB of disk space. Output includes raw frames, paired timestamps, per-round quality checks and statistical summaries.

## Landing on-chain race

```sh
# Preparation only: reads the chain and signs locally; does not broadcast.
python3 landing/prepare.py
# Live mainnet sends: 100 rounds, five candidates per nonce.
python3 landing/race.py
python3 landing/analyze.py
```

Preparation creates 500 signed candidates for 100 consecutive nonces. Each round uses five distinct zero-value self-transfers with the same sender, nonce and gas settings, and equal-length nonzero test data. Candidate-to-path assignment is randomized with a fixed seed. The script checks chain ID 4663, wallet balance, absence of pending nonces and a maximum total gas budget of 0.001 ETH. It refuses to overwrite prepared inputs. Actual gas is charged for included transactions.

Two Node1 Ohio endpoints compete against three user-configured official sequencer IPs. Official Host/SNI is `sequencer.mainnet.chain.robinhood.com`. All requests are prepared in memory; five independent pinned senders prewarm verified TLS connections and busy-poll for 30 seconds before the first round. They keep polling between rounds. A shared monotonic target releases each round; the coordinator waits for the previous nonce's successful receipt before releasing the next.

Winning hashes are identified from successful chain receipts, not HTTP acknowledgements. All 100 rounds are retained; application send-start skew above 100 µs is flagged, not discarded. Results include path wins, gas, timestamps and response counts. This is a same-nonce competition: admission/replacement rules and Node1 internal fan-out affect outcomes. It does not measure milliseconds of propagation advantage or guarantee performance in other regions.

A lock and `started.json` prevent automatic replay. Do not delete the marker to restart a partially sent run. Inspect the chain and prepare a fresh experiment instead. Keep `transactions.json` locally until analysis finishes; signed transactions can be broadcast by anyone who obtains them.

## Run independently of SSH

From the configured shell above, with the virtual environment active:

```sh
mkdir -p data
nohup python3 -u feed/benchmark.py > data/feed.log 2>&1 < /dev/null &
# For Landing, prepare interactively first, then run instead:
nohup python3 -u landing/race.py > data/landing.log 2>&1 < /dev/null &
```

These are finite runs, not recurring jobs. Record the PID printed by your shell. Do not run both examples together when comparing timing, and do not add an automatic retry loop to the Landing sender.

## Tests and privacy

```sh
python3 -m unittest discover -s feed -p 'test_*.py'
python3 -m unittest discover -s landing -p 'test_*.py'
```

Tests are offline and do not send transactions. `.gitignore` excludes `.env`, secrets, private-key files, signed candidates, output directories, raw frames, archives and logs. `.env.example` contains placeholders only. Keep outputs under `data/`; outputs may contain public wallet addresses, transaction hashes, peer addresses and response details, so review them before sharing. Git ignore rules are not a secret scanner and do not untrack previously committed files.
