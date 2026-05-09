-- clubrunner state schema. SQLite, WAL mode.
-- The single point of truth for the agent's domain.

PRAGMA foreign_keys = ON;

-- ==== AUDIT / ORCHESTRATION ====

CREATE TABLE IF NOT EXISTS cycles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    items_in        INTEGER DEFAULT 0,
    items_classified INTEGER DEFAULT 0,
    drafts_created  INTEGER DEFAULT 0,
    broadcasts_sent INTEGER DEFAULT 0,
    decisions_opened INTEGER DEFAULT 0,
    policies_fired  INTEGER DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS actions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id    INTEGER REFERENCES cycles(id),
    ts          TEXT NOT NULL,
    phase       TEXT NOT NULL,        -- collect|triage|brain|execute|reflect
    action      TEXT NOT NULL,
    target_kind TEXT,
    target_id   TEXT,
    ok          INTEGER NOT NULL DEFAULT 1,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_actions_cycle ON actions(cycle_id);
CREATE INDEX IF NOT EXISTS idx_actions_ts    ON actions(ts);

-- ==== INPUTS ====

CREATE TABLE IF NOT EXISTS inputs (
    id           TEXT PRIMARY KEY,        -- f"{source}:{source_id}"
    source       TEXT NOT NULL,           -- imap|playhq|fb|sms|teamapp|gcal|council|bom_heat|bom_lightning
    source_id    TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    sender       TEXT,
    subject      TEXT,
    body         TEXT,
    raw_json     TEXT,
    classification TEXT,                  -- set by triage
    sentiment    TEXT,                    -- pos|neu|neg|hostile
    safeguarding INTEGER DEFAULT 0,
    priority     INTEGER DEFAULT 0,       -- 0..100
    status       TEXT NOT NULL DEFAULT 'new',  -- new|classified|drafted|escalated|done|ignored
    UNIQUE(source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_inputs_status ON inputs(status);
CREATE INDEX IF NOT EXISTS idx_inputs_class  ON inputs(classification);

-- ==== CLUB DOMAIN ====

CREATE TABLE IF NOT EXISTS grounds (
    id          TEXT PRIMARY KEY,          -- e.g. 'henley_oval'
    name        TEXT NOT NULL,
    council     TEXT,
    postcode    TEXT,
    lat         REAL,
    lng         REAL,
    surface     TEXT,
    quirks      TEXT,                       -- free-text notes
    drainage_threshold_mm REAL              -- closes after >N mm overnight rain (NULL = no policy)
);

CREATE TABLE IF NOT EXISTS teams (
    id              TEXT PRIMARY KEY,       -- e.g. 'u13_girls'
    name            TEXT NOT NULL,
    age_grade       TEXT NOT NULL,          -- u8 u9 u10 ... u17 auskick
    division        TEXT,                   -- mixed|boys|girls|auskick
    home_ground_id  TEXT REFERENCES grounds(id),
    coach_id        TEXT,
    manager_id      TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS people (
    id              TEXT PRIMARY KEY,
    role            TEXT NOT NULL,         -- parent|coach|manager|committee|umpire|sponsor_rep
    name            TEXT NOT NULL,
    email           TEXT,
    sms             TEXT,
    fb_user         TEXT,
    teamapp_user    TEXT,
    preferred_channel TEXT DEFAULT 'email',
    quiet_hours     TEXT,
    wwcc_expiry     TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS players (
    id              TEXT PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    dob             TEXT NOT NULL,
    team_id         TEXT REFERENCES teams(id),
    parent_id       TEXT REFERENCES people(id),
    ffa_number      TEXT,
    rego_status     TEXT NOT NULL DEFAULT 'pending', -- pending|paid|insured|active|inactive
    rego_paid_at    TEXT,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_players_rego ON players(rego_status);

CREATE TABLE IF NOT EXISTS fixtures (
    id              TEXT PRIMARY KEY,
    round           INTEGER NOT NULL,
    team_id         TEXT NOT NULL REFERENCES teams(id),
    opponent        TEXT NOT NULL,
    ground_id       TEXT REFERENCES grounds(id),
    kickoff         TEXT NOT NULL,         -- ISO datetime
    home_or_away    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'scheduled', -- scheduled|cancelled|paused|completed|forfeit
    status_reason   TEXT,
    home_score      INTEGER,
    away_score      INTEGER,
    last_status_change TEXT
);
CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff ON fixtures(kickoff);
CREATE INDEX IF NOT EXISTS idx_fixtures_team    ON fixtures(team_id);

CREATE TABLE IF NOT EXISTS duties (
    id              TEXT PRIMARY KEY,
    fixture_id      TEXT NOT NULL REFERENCES fixtures(id),
    role            TEXT NOT NULL,         -- canteen|bbq|scoreboard|gate|first_aid
    person_id       TEXT REFERENCES people(id),
    confirmed       INTEGER DEFAULT 0,
    last_reminded_at TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
    id              TEXT PRIMARY KEY,
    fixture_id      TEXT REFERENCES fixtures(id),
    player_id       TEXT REFERENCES players(id),
    kind            TEXT NOT NULL,         -- injury|behaviour|crowd|equipment|safeguarding
    severity        TEXT NOT NULL,         -- minor|moderate|serious|critical
    summary         TEXT NOT NULL,
    detected_at     TEXT NOT NULL,
    league_report_due TEXT,
    league_report_at TEXT,
    status          TEXT NOT NULL DEFAULT 'open', -- open|reported|closed
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS sponsors (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    tier            TEXT NOT NULL,          -- major|minor
    contract_value  REAL,
    contract_end    TEXT,
    posts_owed      INTEGER DEFAULT 0,
    last_post_at    TEXT,
    rep_id          TEXT REFERENCES people(id)
);

-- ==== AGENT WORK ====

CREATE TABLE IF NOT EXISTS broadcasts (
    id              TEXT PRIMARY KEY,
    cycle_id        INTEGER REFERENCES cycles(id),
    playbook        TEXT,                   -- heat_policy|lightning_pause|...|adhoc
    channel         TEXT NOT NULL,          -- sms|email|fb_post|teamapp_post|website
    segment         TEXT NOT NULL,          -- e.g. 'team:u13_girls' | 'all_parents'
    reach           INTEGER NOT NULL,
    subject         TEXT,
    body            TEXT NOT NULL,
    status          TEXT NOT NULL,          -- drafted|gated|approved|sent|cancelled
    blocked_by      TEXT,                   -- gate name
    created_at      TEXT NOT NULL,
    decided_at      TEXT,
    sent_at         TEXT,
    related_input   TEXT,
    -- meta_json: domain side-effects to apply when this broadcast actually
    -- transitions to 'sent'. The newsletter playbook stores the spotlighted
    -- sponsor here so that posts_owed only decrements after the newsletter
    -- has actually gone out (i.e. Diane approved the >50-reach gate). At
    -- draft time the broadcast is 'gated', not 'sent' — applying the credit
    -- there would silently rotate sponsors even if Diane declined the
    -- newsletter.
    meta_json       TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
    id              TEXT PRIMARY KEY,
    cycle_id        INTEGER REFERENCES cycles(id),
    kind            TEXT NOT NULL,          -- approval_required|incident_brief|complaint_review|sponsor_dispute|...
    target_kind     TEXT,
    target_id       TEXT,
    summary         TEXT NOT NULL,
    context         TEXT NOT NULL,
    options_json    TEXT NOT NULL,
    default_option  TEXT,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending|resolved|expired
    chosen          TEXT,
    resolved_at     TEXT,
    resolved_note   TEXT,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);

-- ==== POLICY / WEATHER ====

CREATE TABLE IF NOT EXISTS weather_state (
    ground_id       TEXT NOT NULL,
    observed_at     TEXT NOT NULL,
    forecast_max_c  REAL,
    forecast_for    TEXT,
    lightning_km    REAL,                   -- distance of nearest strike, NULL = none
    rain_24h_mm     REAL,                   -- rolling 24h rainfall to obs time, NULL = unknown
    PRIMARY KEY(ground_id, observed_at)
);

CREATE TABLE IF NOT EXISTS policy_runs (
    id              TEXT PRIMARY KEY,       -- idempotency key, e.g. 'lightning_pause:henley_oval:2026-05-09T10:00'
    playbook        TEXT NOT NULL,
    target_kind     TEXT,
    target_id       TEXT,
    cycle_id        INTEGER REFERENCES cycles(id),
    fired_at        TEXT NOT NULL,
    outcome         TEXT NOT NULL,          -- drafted|sent|skipped_dedup|gated
    detail          TEXT
);

-- ==== HEALTH ====

CREATE TABLE IF NOT EXISTS breakers (
    integration     TEXT PRIMARY KEY,       -- imap|playhq|fb|twilio|bom|council|teamapp
    state           TEXT NOT NULL DEFAULT 'closed', -- closed|open|half_open
    consecutive_fail INTEGER DEFAULT 0,
    opened_at       TEXT,
    cooldown_until  TEXT,
    last_error      TEXT,
    last_success_at TEXT
);

-- ==== MEMORY ====

CREATE TABLE IF NOT EXISTS parent_memory (
    parent_id       TEXT PRIMARY KEY REFERENCES people(id),
    complaint_count INTEGER DEFAULT 0,
    last_complaint_at TEXT,
    sentiment_trend TEXT DEFAULT 'neutral',
    channel_pref    TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS coach_memory (
    coach_id        TEXT PRIMARY KEY REFERENCES people(id),
    reports_due     INTEGER DEFAULT 0,
    reports_submitted INTEGER DEFAULT 0,
    no_shows        INTEGER DEFAULT 0,
    reliability     REAL DEFAULT 1.0,
    notes           TEXT
);

-- ==== MATCH REPORTS ====
-- Coaches email match reports to the secretary inbox after each round.
-- The agent ingests them, parses the team + score from the body, stamps
-- the matching fixture's score (when missing), credits coach reliability,
-- drafts a per-team channel post (gated by reach), and the Sunday
-- newsletter pulls a short blurb from the most recent reports.
CREATE TABLE IF NOT EXISTS match_reports (
    id                  TEXT PRIMARY KEY,
    fixture_id          TEXT REFERENCES fixtures(id),
    team_id             TEXT REFERENCES teams(id),
    coach_id            TEXT REFERENCES people(id),
    round               INTEGER,
    body                TEXT NOT NULL,
    parsed_score_for    INTEGER,
    parsed_score_against INTEGER,
    submitted_at        TEXT NOT NULL,
    source_input        TEXT,
    posted_broadcast_id TEXT,
    status              TEXT NOT NULL DEFAULT 'received'
);
CREATE INDEX IF NOT EXISTS idx_match_reports_fixture
    ON match_reports(fixture_id);
CREATE INDEX IF NOT EXISTS idx_match_reports_team
    ON match_reports(team_id);

-- ==== CALENDAR EVENTS ====
-- Diane runs the club calendar in Google Calendar — committee meetings,
-- working bees, AGM, special events (presentation night, gala day),
-- training rescheduled to a different ground when council mows. The
-- gcal collector mirrors those events here, then mints reminder-tick
-- inputs at 7d/1d/2h so the event_reminder playbook can fire timely
-- broadcasts to the right audience. The newsletter + committee_brief
-- read directly from this table for the "upcoming" sections.
--
-- audience hint:
--   committee  -> 7 portfolio members (notify via email)
--   all_club   -> all-parents broadcast (gated by reach if >50)
--   coaches    -> coach + manager roster
--   team:<tid> -> a single team's parents
--   diane      -> personal — Diane only
CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,        -- 'evt_<uid12>'
    source_uid      TEXT NOT NULL UNIQUE,    -- gcal event UID, stable across edits
    summary         TEXT NOT NULL,
    description     TEXT,
    location        TEXT,
    start_at        TEXT NOT NULL,
    end_at          TEXT,
    audience        TEXT NOT NULL DEFAULT 'committee',
    team_id         TEXT REFERENCES teams(id),
    last_seen_at    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'scheduled',  -- scheduled|cancelled|past
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_start ON events(start_at);
CREATE INDEX IF NOT EXISTS idx_events_audience ON events(audience);
