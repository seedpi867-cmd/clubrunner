# clubrunner

The volunteer secretary's deputy for a junior football club.

Watches the league inbox, BOM weather, the council booking portal, the
registration platform, the schedule clock, and the parent comms
channels. Drafts and sends the routine work. Pauses play when lightning
strikes. Escalates anything that needs human judgment.

Built for **Diane Pasquale**, volunteer Secretary at Henley & Grange
Junior Football Club (240 juniors, 14 teams, 3 grounds, 26-week
season). Generalises to any junior sports club run by a single
overworked volunteer.

## What it does autonomously

- Lightning pause/resume (BOM radar within 10km → SMS + post +
  pause fixtures, override quiet hours)
- Heat policy drafts for U8/U9/Auskick (BOM forecast >36°C)
- Duty roster reminders (3 days / 1 day / 2 hours before kickoff)
- Registration payment chase, 3-stage cadence
- WWCC renewal reminders to coaches (90/60/30/14/7/1 days)
- Routine FAQ replies on Facebook/email
- Player availability acknowledgements
- Match report ingestion from coaches — score parsed, fixture
  stamped, per-team channel post drafted, coach reliability
  credited, quote folded into Sunday newsletter
- Sunday newsletter assembly
- Sunday-night committee briefing (weekly digest to all committee
  portfolios — incidents, queue, payments, WWCC, sponsors, agent health)
- Saturday 06:00 match-day briefing — personal email to Diane with
  today's fixtures, live weather per ground, unconfirmed duty slots,
  prior round's outstanding match reports, and decisions still on her
  queue that touch today's run of play
- Sponsor banner rotation per contract
- Insurance certificate forwarding

## What it escalates to Diane

- Safeguarding signals (pattern-flagged, never autosent)
- Negative-sentiment complaints
- Refund requests over $200
- Sponsor disputes / contract changes
- Council booking disputes
- Coach resignations or no-shows
- Any broadcast over 50 recipients (one-tap approve)
- Anything "policy says human required"

## Architecture

```
COLLECT → TRIAGE → BRAIN → EXECUTE → REFLECT
```

- **edge/collectors/** — IMAP, BOM heat, BOM lightning, PlayHQ,
  council portal, schedule clock. Real watchers.
- **edge/state/** — SQLite (WAL). Single source of truth. Schema in
  `schema.sql`.
- **edge/gates/** — hours, dedup, rate limit, broadcast threshold,
  SMS cost, safeguarding.
- **edge/health/** — circuit breakers per integration.
- **triage/** — rules-first (`rules.yaml`); LLM only for novel.
- **brain/** — Claude CLI for novel/high-stakes; returns drafts or
  decisions, never sends.
- **tools/** — playbooks (heat_policy, lightning_policy,
  duty_reminder, payment_chase, wwcc_check, newsletter, simple
  acks); broadcast helper that runs every outbound through gates.
- **dashboard/** — stdlib HTTP server, live HTML, one-tap decisions,
  per-broadcast detail view (`/broadcast/<id>`) so Diane can read
  exactly what the agent said in her name.
- **memory/** — working snapshot, short-term log, long-term JSON
  (parents, sponsors, grounds, coaches, weather history, incidents
  history).

## 10 minutes from clone to running

```bash
git clone <repo> clubrunner
cd clubrunner
./start.sh
# open http://127.0.0.1:8089/
```

The first cycle runs immediately, seeds the club state, processes the
sample scenario in `scenarios/today.json` (38°C heat warning + an
isolated lightning cell near Henley), and shows you Diane's decision
queue.

To stop:

```bash
./stop.sh
```

## Configure policy in `config.yaml`

Diane sets the policy. You change behaviour by editing `config.yaml`:

- `heat_policy.cancel_threshold_c` — when to draft a cancellation
- `lightning_policy.pause_distance_km` — strike distance to pause
- `broadcast.approval_threshold_recipients` — when a send needs a tap
- `broadcast.daily_sms_cap_aud` — Twilio spend ceiling
- `broadcast.dedup_window_minutes.<playbook>` — anti-spam windows
- `safeguarding.trigger_patterns` — phrases that escalate
- `cycle.interval_minutes` / `match_day_interval_minutes`
- collector enable flags + endpoints

## Going live

The agent runs end-to-end on stub data out of the box. To wire real
integrations:

- **IMAP**: set `collectors.imap.host` and credentials via env;
  `imap_secretary.py` already handles IDLE and a local maildir
  fallback.
- **BOM heat**: switch `collectors.bom_heat.forecast_endpoint` from
  `stub` to the BOM JSON endpoint.
- **BOM lightning**: requires WZRPC subscription; provided as stub.
- **PlayHQ**: set `collectors.playhq.base_url`.
- **Twilio outbound**: replace `tools/broadcast._wire_send` with the
  Twilio call; everything upstream is unchanged.
- **Facebook Graph**: set the page token, flip
  `collectors.facebook.enabled` to true.

The orchestration is real. The wire-out is the only thing Diane plugs
in.

## Files

```
orchestrator.py       five-phase loop
config.yaml           policy
config.py             yaml loader (no PyYAML required)
edge/state/           SQLite + schema + seed
edge/collectors/      6 collectors
edge/gates/           6 gates
edge/health/          breakers
triage/               classifier, router, rules
brain/executive.py    Claude CLI caller (dry-run by default)
tools/                playbooks + broadcast helper
memory/               working / short / long term
dashboard/            HTTP server + live HTML
scenarios/today.json  fixture data for stub mode
inputs/maildir/       fallback local maildir
tests/                self-tests
start.sh / stop.sh    process control
```

## Tests

```bash
for t in tests/test_*.py; do python3 "$t"; done
```

- `test_approval_flow.py` — >50-reach broadcast, threshold gate,
  one-tap approve via direct call and via the dashboard's `POST /decide`
  HTTP endpoint.
- `test_brain_hold.py` — brain-drafted replies route through gates,
  safeguarding never autosends.
- `test_incident_flow.py` — injury input → incident row, league
  deadline tracking, idempotency.
- `test_newsletter_sponsor.py` — sponsor credits only fire on actual
  send, rotation works.
- `test_committee_brief.py` — Sunday digest reaches all 7 committee
  members, dedup silently blocks re-fires, schedule tick mints once
  per ISO week.
- `test_match_report.py` — coach reports parsed (AFL ladders + plain
  scores + lost/won verb-flip), fixture score stamped when missing,
  newsletter weaves the coach quote, unknown senders open a verify
  decision instead of broadcasting.

## Dependencies

- Python 3.11+
- SQLite (stdlib)
- Claude CLI on PATH (optional; brain runs in dry-run by default)
- ollama (optional; triage is rule-first and works without it)

No pip installs required. PyYAML is optional — `config.py` ships its
own minimal YAML loader.
