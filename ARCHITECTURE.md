# clubrunner — Architecture

A junior sports club secretary's autonomous deputy.

The agent runs the weekly cycle of a 240-member junior football club —
fixtures, weather policy, parent comms, registrations, duty rosters, the
newsletter, sponsor obligations, incident tracking — so that Diane
(volunteer Secretary, Henley & Grange JFC) gets her Sundays back. She
remains the executive: she sets policy, approves >50-recipient broadcasts,
and handles incidents. Everything else, clubrunner does.

## Single point of truth

`edge/state/state.db` (SQLite, WAL). Tables:

- `cycles` — every COLLECT→REFLECT run
- `actions` — audit log of every phase event
- `inputs` — raw collected items (uniqued by source+source_id)
- `fixtures` — round, team_id, opponent, ground_id, kickoff, status
- `teams` — id, name, age_grade, manager_contact, coach_contact
- `players` — registration data, age, team_id, parent_id, ffa_number
- `parents` — name, contacts (email/sms/fb), preferred channel, sentiment
- `coaches` — name, contacts, wwcc_expiry, reliability score
- `grounds` — id, name, council, quirks, lat/lng (for BOM cell match)
- `incidents` — type, severity, players involved, league_report_due
- `broadcasts` — segment, channel, body, status (drafted/approved/sent), reach
- `sponsors` — name, tier, contract_obligations_remaining
- `decisions` — human queue (pending/resolved)
- `weather_state` — latest BOM heat + lightning per ground
- `policy_runs` — all playbook/policy invocations with idempotency keys (replaces a separate `playbooks` table — same data, clearer name)
- `breakers` — circuit breaker state per integration

## The 5-phase cycle

```
COLLECT → TRIAGE → BRAIN → EXECUTE → REFLECT
```

### COLLECT — watches real sources, no human feeding

| Collector             | Source                                       | Cadence       |
|-----------------------|----------------------------------------------|---------------|
| `bom_heat.py`         | BOM 7-day forecast JSON per ground postcode  | hourly        |
| `bom_lightning.py`    | BOM lightning observations / 10-min radar    | every 10 min on match days |
| `imap_secretary.py`   | IMAP IDLE on secretary@henleygrangejfc.com.au | continuous    |
| `playhq.py`           | PlayHQ club portal (regs, payments, results) | every 15 min  |
| `council_portal.py`   | City of Charles Sturt booking portal         | every 60 min  |
| `facebook_page.py`    | FB Graph API: page messages + post comments  | every 5 min   |
| `teamapp.py`          | TeamApp API (or scrape)                      | every 15 min  |
| `sms_inbound.py`      | Twilio webhook → state.db                    | event-driven  |
| `gcal.py`             | Google Calendar — club calendar              | every 30 min  |
| `schedule.py`         | Time-based playbook triggers (newsletter, duty roster reminders, WWCC, payment chases) | every cycle |

Each collector writes to `inputs` with a stable `(source, source_id)` so
re-running is idempotent.

### TRIAGE — local, cheap, deterministic-first

`triage/classifier.py` first applies `triage/rules.yaml` (regex / keyword /
sender-domain / sentiment). Items that match a clean rule never reach the
brain. Unmatched items optionally go through ollama for category +
sentiment + safeguarding-signal flags.

Categories: `fixture_change`, `weather_policy`, `registration`,
`payment`, `complaint`, `incident`, `safeguarding`, `sponsor`,
`council`, `coach_admin`, `parent_faq_training`, `parent_faq_register`,
`volunteer_signup`, `media`, `junk`.

`triage/prioritizer.py` writes `work_items` with priority + escalate flag
+ playbook hint.

### BRAIN — only for novel / high-stakes

The brain receives only items that:
- the classifier flagged escalate, or
- have safeguarding signals, or
- have negative-sentiment + complaint kind, or
- are sponsor disputes / council disputes / contract changes.

It returns either a `Draft` (an outbound message awaiting gates) or a
`Decision` (an item for Diane's queue).

### EXECUTE — through gates

Every outbound action flows through stacked gates from `edge/gates/`:

- `hours.py` — no parent SMS outside 07:00–21:00 Adelaide (lightning override exempt)
- `dedup.py` — same target + same playbook + within window = block
- `rate_limit.py` — per-channel + per-recipient per-day caps
- `broadcast_threshold.py` — any broadcast >50 recipients goes to decision queue as one-tap-approve
- `sms_cost.py` — daily/weekly Twilio spend cap
- `sponsor_post.py` — anything tagged with a sponsor goes to approval gate
- `safeguarding.py` — child-safeguarding signals never autosend; always escalate

### Hard automations (no approval gate, by policy)

| Policy            | Trigger                                                    | Action |
|-------------------|------------------------------------------------------------|--------|
| Lightning pause   | BOM observation: lightning <10km of any active ground      | post pause notice + SMS team managers + TeamApp post |
| Lightning resume  | 30 minutes since last strike <10km                         | post resume notice |
| Heat cancel       | BOM forecast >36°C at scheduled match time, U8/U9 only     | drafted, gated through approval (>50 reach), pre-filled |

Lightning is the only true zero-approval automation — it has to be
instant and policy is unambiguous. Heat is drafted but Diane confirms
because forecasts are noisy and the parent-mass-comm cost of a wrong
cancel is high.

### REFLECT — memory, health, dashboard

- `memory/working.json` — current cycle state
- `memory/short_term.jsonl` — last N cycles
- `memory/long_term/` — parent prefs, sponsor relationship state, ground quirks, complaint history, weather decision history, coach reliability
- `edge/health/breakers.py` — circuit breakers per external integration; trip threshold = 3 consecutive failures, cooldown = 10 cycles
- Dashboard is pulled fresh from `state.db` on every page load

## Gate enforcement is real, not advisory

When a gate blocks, the action goes into `decisions` as
`type=approval_required`, with:
- the proposed action
- the gate that blocked
- a one-tap option list (approve / decline / hold)
- the default option

The dashboard renders these as approve buttons that POST back to a small
local handler.

## Memory that compounds

Read at start of every cycle into BRAIN context:

- `parents.json` — `{parent_id: {channel_pref, complaint_count, last_complaint_at, sentiment_trend}}`
- `sponsors.json` — `{sponsor_id: {tier, contract_terms_remaining, last_post_at}}`
- `grounds.json` — `{ground_id: {drainage, closes_after_mm_overnight, surface, postcode, lat, lng}}`
- `weather_history.json` — past forecast vs actual + decision made + outcome
- `coaches.json` — `{coach_id: {report_submission_rate, last_no_show, wwcc_expiry}}`
- `incidents_history.json` — past incident handling, league report timeliness

## What runs autonomously

- Lightning pause / resume posts
- Duty-roster reminders (3 days, 1 day, 2 hours before)
- Registration chase emails (3-stage cadence)
- WWCC renewal reminders to coaches (90/60/30/14/7 days before expiry)
- Routine FAQ replies on Facebook (>0.85 confidence match against FAQ index)
- Match report assembly from coach inputs + result data
- Sunday newsletter draft (assembled and waiting at 17:00 Sunday)
- Saturday 06:00 match-day briefing to Diane — fixtures, live weather
  per ground, unconfirmed duty slots, prior round's outstanding match
  reports, queue items touching today's play
- Sponsor banner rotation per contract spec, plus a delivery-feasibility
  sweep that opens a decision when a sponsor's posts_owed cannot fit in
  the weeks remaining before contract_end / season_end (cooldown by
  sponsor; severity-bypass when shortfall worsens)
- Insurance certificate forwarding to council
- Heat-policy SMS+post drafts ready to one-tap

## What escalates to Diane

- Any incident report (auto-pre-filled, but human submits)
- Negative-sentiment complaints (drafted reply + decision options)
- Sponsor disputes / contract changes
- Council booking disputes (multi-stakeholder)
- Safeguarding signals (any pattern match: coach + child + isolated /
  uncomfortable / private contact)
- Refund requests >$200
- Coach resignations / no-shows
- Any "policy doc says human required" item
- Heat-cancellation send approval (>50 reach broadcast gate)

## What we will NOT build in v1

- Actually pushing to Facebook / SMS / IMAP outbound (we ship draft +
  one-tap with a stubbed sender; replacing the stub with real Twilio /
  Graph creds is config, not code change). The orchestration is real;
  the wire to outbound providers is the easy part Diane plugs in.
- Live PlayHQ scrape (we ship a polling adapter with offline fixture).

The agent's brain — orchestrator, gates, state, BOM policy, decision
queue, memory, dashboard — is real code that runs end-to-end on cycle
1 with the seeded club state.
