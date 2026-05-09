"""Seed Henley & Grange JFC's actual world into the state db.

Idempotent — only seeds if tables are empty. The seed reflects what
Diane actually has to manage: 14 teams, 3 grounds, ~28 coaches/managers,
a sample of players + parents + sponsors. Just enough that the cycle
end-to-end produces real-looking work.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from edge.state.db import connect, now_iso


# ---------- the club's actual setup ----------

GROUNDS = [
    # (id, name, council, postcode, lat, lng, surface, quirks,
    #  drainage_threshold_mm)
    # Drainage threshold extracted from the quirks text — Henley's eastern
    # oval has a documented closure rule that needs a structured trigger
    # so the agent can act on it, not just describe it. The other grounds
    # have no documented threshold (NULL = no auto-closure policy; rain
    # is just a coach call there).
    ("henley_oval", "Henley Oval", "Charles Sturt", "5022",
     -34.9216, 138.4960, "couch_grass",
     "Eastern oval poor drainage — closes after >20mm overnight rain",
     20.0),
    ("grange_reserve", "Grange Reserve", "Charles Sturt", "5022",
     -34.9050, 138.4895, "couch_grass",
     "Shared with cricket Sept-Mar; check booking conflicts after Aug 1",
     None),
    ("fulham_north", "Fulham North Reserve", "West Torrens", "5024",
     -34.9520, 138.5150, "kikuyu",
     "Council mowing Tue mornings — no early training Tue",
     None),
]

TEAMS = [
    # (id, name, age, division, ground)
    ("auskick_a", "Auskick Group A", "auskick", "auskick", "henley_oval"),
    ("auskick_b", "Auskick Group B", "auskick", "auskick", "henley_oval"),
    ("auskick_c", "Auskick Group C", "auskick", "auskick", "grange_reserve"),
    ("u8_mixed",  "U8 Mixed",  "u8",  "mixed", "henley_oval"),
    ("u9_mixed",  "U9 Mixed",  "u9",  "mixed", "henley_oval"),
    ("u10_mixed", "U10 Mixed", "u10", "mixed", "grange_reserve"),
    ("u11_boys",  "U11 Boys",  "u11", "boys",  "fulham_north"),
    ("u12_boys",  "U12 Boys",  "u12", "boys",  "fulham_north"),
    ("u13_boys",  "U13 Boys",  "u13", "boys",  "henley_oval"),
    ("u13_girls", "U13 Girls", "u13", "girls", "grange_reserve"),
    ("u14_boys",  "U14 Boys",  "u14", "boys",  "henley_oval"),
    ("u15_girls", "U15 Girls", "u15", "girls", "fulham_north"),
    ("u16_boys",  "U16 Boys",  "u16", "boys",  "henley_oval"),
    ("u17_girls", "U17 Girls", "u17", "girls", "grange_reserve"),
]

# A representative slice — not all 480 parents, but enough to drive
# realistic broadcast counts in heat/lightning/duty playbooks.
PEOPLE = [
    # ---- coaches & managers (28 in real life; 14 here, one per team) ----
    ("coach_marko",   "coach",     "Marko Petrović",     "marko@example.com",
     "+61400000101", "email", None, "2026-08-15"),
    ("coach_sara",    "coach",     "Sara Wilkins",       "sara.w@example.com",
     "+61400000102", "sms",   None, "2026-04-22"),  # WWCC expiring soon
    ("coach_dean",    "coach",     "Dean O'Brien",       "dean.o@example.com",
     "+61400000103", "email", None, "2027-01-10"),
    ("coach_nina",    "coach",     "Nina Ashworth",      "nina.a@example.com",
     "+61400000104", "email", None, "2026-06-30"),
    ("coach_pete",    "coach",     "Pete Karras",        "pete.k@example.com",
     "+61400000105", "sms",   None, "2026-11-04"),
    ("coach_alex",    "coach",     "Alex Tran",          "alex.t@example.com",
     "+61400000106", "email", None, "2026-05-19"),
    ("coach_jamie",   "coach",     "Jamie Foord",        "jamie.f@example.com",
     "+61400000107", "email", None, "2026-09-02"),
    ("coach_louise",  "coach",     "Louise Connor",      "louise.c@example.com",
     "+61400000108", "sms",   None, "2026-12-15"),
    ("coach_ravi",    "coach",     "Ravi Mehta",         "ravi.m@example.com",
     "+61400000109", "email", None, "2027-03-20"),
    ("coach_helen",   "coach",     "Helen Wu",           "helen.w@example.com",
     "+61400000110", "email", None, "2026-07-25"),
    ("mgr_kelly",     "manager",   "Kelly Jenner",       "kelly.j@example.com",
     "+61400000201", "sms",   None, None),
    ("mgr_anita",     "manager",   "Anita Goldsmith",    "anita.g@example.com",
     "+61400000202", "email", None, None),
    ("mgr_brett",     "manager",   "Brett Tomlinson",    "brett.t@example.com",
     "+61400000203", "sms",   None, None),
    ("mgr_priya",     "manager",   "Priya Shah",         "priya.s@example.com",
     "+61400000204", "email", None, None),

    # ---- committee (7 portfolio members, per club constitution) ----
    ("comm_pres",     "committee", "Geoff Birnie (Pres)", "pres@henleygrangejfc.com.au",
     "+61400000301", "email", None, None),
    ("comm_vp",       "committee", "Tess Marwick (VP)",   "vp@henleygrangejfc.com.au",
     "+61400000302", "email", None, None),
    ("comm_treasurer","committee", "Robin Atkinson (Tr)", "treasurer@henleygrangejfc.com.au",
     "+61400000303", "email", None, None),
    ("comm_registrar","committee", "Megan Holt (Reg)",    "registrar@henleygrangejfc.com.au",
     "+61400000304", "email", None, None),
    ("comm_coaching", "committee", "Phil Vasquez (CoachCoord)", "coaching@henleygrangejfc.com.au",
     "+61400000305", "email", None, None),
    ("comm_comms",    "committee", "Renu Iyer (Comms)",   "comms@henleygrangejfc.com.au",
     "+61400000306", "email", None, None),
    ("comm_volcoord", "committee", "Tony Pirelli (VolCoord)", "volunteers@henleygrangejfc.com.au",
     "+61400000307", "email", None, None),

    # ---- parents (representative sample — channel pref varied) ----
    ("p_amelia",      "parent", "Amelia Chen",      "amelia.chen@gmail.com",
     "+61400000401", "sms",   None, None),
    ("p_marcus",      "parent", "Marcus Hill",      "mhill@bigpond.net.au",
     "+61400000402", "email", None, None),
    ("p_sandra",      "parent", "Sandra Loretti",   "sandra.l@outlook.com",
     "+61400000403", "sms",   "21:00-07:00", None),
    ("p_dimitri",     "parent", "Dimitri Roussos",  "d.roussos@gmail.com",
     "+61400000404", "email", None, None),
    ("p_holly",       "parent", "Holly Underhill",  "holly.u@yahoo.com.au",
     "+61400000405", "sms",   None, None),
    ("p_kev",         "parent", "Kev Dorling",      "kev.d@hotmail.com",
     "+61400000406", "email", None, None),
    ("p_gita",        "parent", "Gita Banerjee",    "gita.b@gmail.com",
     "+61400000407", "sms",   None, None),
    ("p_bryan",       "parent", "Bryan Mosley",     "bryan.mosley@iinet.net.au",
     "+61400000408", "email", None, None),

    # ---- sponsor reps ----
    ("rep_bendigo",   "sponsor_rep", "Naomi Phillips (Bendigo Henley)",
     "naomi.phillips@bendigoadelaide.com.au", "+61400000501", "email", None, None),
    ("rep_hardware",  "sponsor_rep", "Steve Galanos (Henley Hardware)",
     "steve@henleyhardware.com.au", "+61400000502", "email", None, None),
]

# Each player has a parent + team. We seed 30 (enough that a heat
# cancel for U8/U9 has a real reach number, and enough rego_status
# variety to drive payment chases).
PLAYERS = [
    # (id, first, last, dob, team, parent, ffa, status, paid_at)
    ("pl_001", "Liam",   "Chen",      "2017-03-12", "u9_mixed",  "p_amelia", "FFA1001", "active",  "2026-03-01"),
    ("pl_002", "Mira",   "Chen",      "2015-11-04", "u11_boys",  "p_amelia", "FFA1002", "paid",    "2026-03-01"),
    ("pl_003", "Otis",   "Hill",      "2018-06-20", "u8_mixed",  "p_marcus", "FFA1003", "pending", None),
    ("pl_004", "Felix",  "Loretti",   "2014-09-11", "u12_boys",  "p_sandra", "FFA1004", "pending", None),
    ("pl_005", "Stella", "Loretti",   "2016-04-02", "u10_mixed", "p_sandra", "FFA1005", "active",  "2026-02-28"),
    ("pl_006", "Theo",   "Roussos",   "2013-01-18", "u13_boys",  "p_dimitri", "FFA1006", "active",  "2026-02-15"),
    ("pl_007", "Yannis", "Roussos",   "2009-11-30", "u17_girls", "p_dimitri", "FFA1007", "paid",    "2026-02-15"),
    ("pl_008", "Eve",    "Underhill", "2017-08-05", "u9_mixed",  "p_holly",   "FFA1008", "active",  "2026-03-04"),
    ("pl_009", "Noa",    "Underhill", "2018-12-21", "auskick_a", "p_holly",   "FFA1009", "active",  "2026-03-04"),
    ("pl_010", "Jack",   "Dorling",   "2012-02-09", "u14_boys",  "p_kev",     "FFA1010", "pending", None),
    ("pl_011", "Asha",   "Banerjee",  "2018-10-15", "u8_mixed",  "p_gita",    "FFA1011", "active",  "2026-03-08"),
    ("pl_012", "Rohan",  "Banerjee",  "2016-05-22", "u10_mixed", "p_gita",    "FFA1012", "active",  "2026-03-08"),
    ("pl_013", "Beau",   "Mosley",    "2013-07-03", "u13_girls", "p_bryan",   "FFA1013", "paid",    "2026-02-19"),
    ("pl_014", "Ari",    "Mosley",    "2015-01-29", "u11_boys",  "p_bryan",   "FFA1014", "active",  "2026-02-19"),
]

# Round 12 (this Saturday) and Round 13 (next Saturday) — 14 fixtures
# per round across 3 grounds.
#
# Two rounds is what a real club secretary lives with: the league
# publishes 1–2 rounds ahead so the agent can fire 3-day duty reminders
# for the upcoming weekend while the current weekend is still being
# played. With only Round 12 in the seed the agent has zero forward
# visibility — once Saturday's matches transition to completed/cancelled
# the dashboard shows nothing coming up, and duty_reminder has no T-3d
# anchor to fire on for the following Saturday.
def _round_kickoff(team: str, ground: str, hour: int, weeks_ahead: int = 0):
    sat_this = (datetime.now() + timedelta(
        days=(5 - datetime.now().weekday()) % 7)).date()
    sat = sat_this + timedelta(weeks=weeks_ahead)
    return datetime.combine(sat, datetime.min.time()).replace(
        hour=hour, minute=0).isoformat(timespec="seconds")


# (suffix, team_id, opponent, ground, kickoff hour, home/away)
_FIXTURE_BASE = [
    ("auskick_a",  "auskick_a",  "Glenelg JFC Auskick", "henley_oval",   8, "home"),
    ("auskick_b",  "auskick_b",  "Brighton Bombers",    "henley_oval",   8, "home"),
    ("auskick_c",  "auskick_c",  "Marion JFC",          "grange_reserve",9, "home"),
    ("u8",         "u8_mixed",   "Glenelg JFC",         "henley_oval",   9, "home"),
    ("u9",         "u9_mixed",   "Holdfast Bay",        "henley_oval",  10, "home"),
    ("u10",        "u10_mixed",  "Lockleys Demons",     "grange_reserve",10, "home"),
    ("u11",        "u11_boys",   "Marion JFC",          "fulham_north",  9, "home"),
    ("u12",        "u12_boys",   "West Beach",          "fulham_north", 10, "home"),
    ("u13_b",      "u13_boys",   "Glenelg JFC",         "henley_oval",  11, "home"),
    ("u13_g",      "u13_girls",  "Adelaide Lutheran",   "grange_reserve", 9, "home"),
    ("u14",        "u14_boys",   "Lockleys Demons",     "henley_oval",  12, "home"),
    ("u15_g",      "u15_girls",  "Brighton Bombers",    "fulham_north", 12, "home"),
    ("u16",        "u16_boys",   "Marion JFC",          "henley_oval",  14, "home"),
    ("u17_g",      "u17_girls",  "Holdfast Bay",        "grange_reserve",11, "home"),
]

# Round 13 mirrors Round 12 but with the venues swapped (away rounds
# are real life — the home/away pattern alternates) so the dashboard
# shows realistic ground variety and the drainage policy doesn't fire
# on grounds the club isn't actually using next week.
_R13_OVERRIDES = {
    "auskick_a":  ("Brighton Bombers Auskick", None,             "away"),
    "auskick_b":  ("Marion JFC Auskick",       None,             "away"),
    "auskick_c":  ("Henley Auskick combined",  "henley_oval",    "home"),
    "u8":         ("Holdfast Bay",             None,             "away"),
    "u9":         ("Lockleys Demons",          "grange_reserve", "home"),
    "u10":        ("Brighton Bombers",         "henley_oval",    "home"),
    "u11":        ("West Beach",               None,             "away"),
    "u12":        ("Marion JFC",               "fulham_north",   "home"),
    "u13_b":      ("Adelaide Lutheran",        "grange_reserve", "home"),
    "u13_g":      ("Marion JFC Girls",         None,             "away"),
    "u14":        ("Glenelg JFC",              None,             "away"),
    "u15_g":      ("Lockleys Demons Girls",    "henley_oval",    "home"),
    "u16":        ("Brighton Bombers",         None,             "away"),
    "u17_g":      ("Adelaide Lutheran",        "fulham_north",   "home"),
}


def _fixtures():
    out = []
    for suffix, team, opp, gid, hour, hoa in _FIXTURE_BASE:
        fid = f"fx_r12_{suffix}"
        out.append((fid, 12, team, opp, gid,
                    _round_kickoff(team, gid, hour, 0), hoa))
    for suffix, team, opp_default, gid_default, hour, _ in _FIXTURE_BASE:
        opp, gid_override, hoa = _R13_OVERRIDES[suffix]
        gid = gid_override if gid_override is not None else gid_default
        fid = f"fx_r13_{suffix}"
        out.append((fid, 13, team, opp, gid,
                    _round_kickoff(team, gid, hour, 1), hoa))
    return out


SPONSORS = [
    ("sp_bendigo",  "Bendigo Bank Henley & Grange", "major", 12000.0,
     "2026-12-31", 8, "rep_bendigo"),
    ("sp_hardware", "Henley Hardware",              "minor", 2500.0,
     "2026-12-31", 4, "rep_hardware"),
]


# ---------- duty rosters ----------
# Each home fixture has canteen + gate + scoreboard. Larger grade fixtures
# (U13+) also get BBQ. Rostered to parents (and managers/committee for the
# small grades where parents are thin on the ground). About 1-in-3 already
# confirmed via TeamApp; the rest are unconfirmed and will be reminded by
# duty_reminder at 3d / 1d / 2h. The mix lets a real cycle exercise the
# T-2h escalation path on at least one fixture without needing a test
# harness to mint duties.
#
# (fixture_id, role, person_id, confirmed)
DUTIES = [
    # Auskick (small grades — managers + committee fill in)
    ("fx_r12_auskick_a", "canteen",    "p_amelia",     1),
    ("fx_r12_auskick_a", "gate",       "mgr_kelly",    1),
    ("fx_r12_auskick_a", "scoreboard", "p_holly",      0),
    ("fx_r12_auskick_b", "canteen",    "p_gita",       0),
    ("fx_r12_auskick_b", "gate",       "comm_volcoord", 1),
    ("fx_r12_auskick_b", "scoreboard", "p_marcus",     0),
    ("fx_r12_auskick_c", "canteen",    "mgr_kelly",    0),
    ("fx_r12_auskick_c", "gate",       "p_kev",        1),
    ("fx_r12_auskick_c", "scoreboard", "p_bryan",      0),
    # U8 / U9 / U10
    ("fx_r12_u8",        "canteen",    "p_gita",       1),
    ("fx_r12_u8",        "gate",       "p_marcus",     0),
    ("fx_r12_u8",        "scoreboard", "mgr_anita",    1),
    ("fx_r12_u9",        "canteen",    "p_holly",      1),
    ("fx_r12_u9",        "gate",       "p_amelia",     0),
    ("fx_r12_u9",        "scoreboard", "p_dimitri",    0),
    ("fx_r12_u10",       "canteen",    "p_sandra",     0),
    ("fx_r12_u10",       "gate",       "p_gita",       0),
    ("fx_r12_u10",       "scoreboard", "p_bryan",      1),
    # U11 / U12 (BBQ kicks in)
    ("fx_r12_u11",       "canteen",    "p_amelia",     0),
    ("fx_r12_u11",       "bbq",        "mgr_brett",    1),
    ("fx_r12_u11",       "gate",       "p_bryan",      0),
    ("fx_r12_u11",       "scoreboard", "p_kev",        0),
    ("fx_r12_u12",       "canteen",    "p_sandra",     1),
    ("fx_r12_u12",       "bbq",        "p_marcus",     0),
    ("fx_r12_u12",       "gate",       "p_kev",        0),
    ("fx_r12_u12",       "scoreboard", "comm_coaching", 0),
    # U13 / U14 / U15
    ("fx_r12_u13_b",     "canteen",    "p_dimitri",    1),
    ("fx_r12_u13_b",     "bbq",        "p_kev",        0),
    ("fx_r12_u13_b",     "gate",       "comm_vp",      1),
    ("fx_r12_u13_b",     "scoreboard", "p_bryan",      0),
    ("fx_r12_u13_g",     "canteen",    "p_bryan",      0),
    ("fx_r12_u13_g",     "bbq",        "mgr_priya",    0),
    ("fx_r12_u13_g",     "gate",       "p_sandra",     1),
    ("fx_r12_u13_g",     "scoreboard", "p_amelia",     0),
    ("fx_r12_u14",       "canteen",    "p_kev",        0),
    ("fx_r12_u14",       "bbq",        "comm_pres",    1),
    ("fx_r12_u14",       "gate",       "p_marcus",     0),
    ("fx_r12_u14",       "scoreboard", "p_holly",      0),
    ("fx_r12_u15_g",     "canteen",    "mgr_priya",    1),
    ("fx_r12_u15_g",     "bbq",        "p_holly",      0),
    ("fx_r12_u15_g",     "gate",       "p_dimitri",    0),
    ("fx_r12_u15_g",     "scoreboard", "p_gita",       0),
    # U16 / U17
    ("fx_r12_u16",       "canteen",    "p_marcus",     0),
    ("fx_r12_u16",       "bbq",        "comm_treasurer", 1),
    ("fx_r12_u16",       "gate",       "p_amelia",     1),
    ("fx_r12_u16",       "scoreboard", "p_dimitri",    0),
    ("fx_r12_u17_g",     "canteen",    "p_dimitri",    0),
    ("fx_r12_u17_g",     "bbq",        "p_holly",      0),
    ("fx_r12_u17_g",     "gate",       "comm_registrar", 1),
    ("fx_r12_u17_g",     "scoreboard", "mgr_priya",    0),

    # Round 13 (next Saturday) — duties seeded only for home fixtures.
    # Most start unconfirmed: that's the realistic state mid-week before a
    # round, and it lets the T-3d duty reminder anchor have something to
    # actually chase. A few confirmations are sprinkled in (parents who
    # tap "yes" the moment the roster lands) so the dashboard isn't
    # dominated by a single unconfirmed wall.
    ("fx_r13_auskick_c", "canteen",    "mgr_kelly",    0),
    ("fx_r13_auskick_c", "gate",       "p_amelia",     0),
    ("fx_r13_auskick_c", "scoreboard", "p_holly",      1),
    ("fx_r13_u9",        "canteen",    "p_holly",      0),
    ("fx_r13_u9",        "gate",       "p_dimitri",    0),
    ("fx_r13_u9",        "scoreboard", "mgr_anita",    0),
    ("fx_r13_u10",       "canteen",    "p_sandra",     0),
    ("fx_r13_u10",       "gate",       "p_gita",       1),
    ("fx_r13_u10",       "scoreboard", "p_bryan",      0),
    ("fx_r13_u12",       "canteen",    "p_marcus",     0),
    ("fx_r13_u12",       "bbq",        "comm_coaching", 0),
    ("fx_r13_u12",       "gate",       "p_kev",        0),
    ("fx_r13_u12",       "scoreboard", "p_dimitri",    1),
    ("fx_r13_u13_b",     "canteen",    "p_kev",        0),
    ("fx_r13_u13_b",     "bbq",        "comm_vp",      0),
    ("fx_r13_u13_b",     "gate",       "p_marcus",     0),
    ("fx_r13_u13_b",     "scoreboard", "p_amelia",     0),
    ("fx_r13_u15_g",     "canteen",    "mgr_priya",    0),
    ("fx_r13_u15_g",     "bbq",        "p_holly",      0),
    ("fx_r13_u15_g",     "gate",       "p_gita",       0),
    ("fx_r13_u15_g",     "scoreboard", "p_dimitri",    0),
    ("fx_r13_u17_g",     "canteen",    "p_dimitri",    0),
    ("fx_r13_u17_g",     "bbq",        "comm_registrar", 1),
    ("fx_r13_u17_g",     "gate",       "p_amelia",     0),
    ("fx_r13_u17_g",     "scoreboard", "mgr_priya",    0),
]


# ---------- coach -> team binding (so we can pull the coach for a team) ----------
TEAM_COACH_MGR = [
    ("auskick_a",  "coach_marko",  "mgr_kelly"),
    ("auskick_b",  "coach_sara",   "mgr_kelly"),
    ("auskick_c",  "coach_dean",   "mgr_kelly"),
    ("u8_mixed",   "coach_nina",   "mgr_anita"),
    ("u9_mixed",   "coach_pete",   "mgr_anita"),
    ("u10_mixed",  "coach_alex",   "mgr_anita"),
    ("u11_boys",   "coach_jamie",  "mgr_brett"),
    ("u12_boys",   "coach_louise", "mgr_brett"),
    ("u13_boys",   "coach_ravi",   "mgr_brett"),
    ("u13_girls",  "coach_helen",  "mgr_priya"),
    ("u14_boys",   "coach_ravi",   "mgr_brett"),
    ("u15_girls",  "coach_helen",  "mgr_priya"),
    ("u16_boys",   "coach_marko",  "mgr_brett"),
    ("u17_girls",  "coach_dean",   "mgr_priya"),
]


def _is_empty(conn) -> bool:
    n = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    return n == 0


def seed() -> None:
    conn = connect()
    if not _is_empty(conn):
        conn.close()
        return

    for g in GROUNDS:
        conn.execute(
            "INSERT INTO grounds(id, name, council, postcode, lat, lng, "
            "surface, quirks, drainage_threshold_mm) "
            "VALUES(?,?,?,?,?,?,?,?,?)", g)

    for p in PEOPLE:
        conn.execute(
            "INSERT INTO people(id, role, name, email, sms, "
            "preferred_channel, quiet_hours, wwcc_expiry) "
            "VALUES(?,?,?,?,?,?,?,?)", p)

    for t in TEAMS:
        tid, name, age, div, ground = t
        conn.execute(
            "INSERT INTO teams(id, name, age_grade, division, "
            "home_ground_id) VALUES(?,?,?,?,?)",
            (tid, name, age, div, ground))

    for tid, coach, mgr in TEAM_COACH_MGR:
        conn.execute("UPDATE teams SET coach_id=?, manager_id=? WHERE id=?",
                     (coach, mgr, tid))

    for pl in PLAYERS:
        conn.execute(
            "INSERT INTO players(id, first_name, last_name, dob, team_id, "
            "parent_id, ffa_number, rego_status, rego_paid_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)", pl)

    for fx in _fixtures():
        fid, rnd, tid, opp, gid, kickoff, hoa = fx
        conn.execute(
            "INSERT INTO fixtures(id, round, team_id, opponent, ground_id, "
            "kickoff, home_or_away, status, last_status_change) "
            "VALUES(?,?,?,?,?,?,?, 'scheduled', ?)",
            (fid, rnd, tid, opp, gid, kickoff, hoa, now_iso()))

    for sp in SPONSORS:
        conn.execute(
            "INSERT INTO sponsors(id, name, tier, contract_value, "
            "contract_end, posts_owed, rep_id) VALUES(?,?,?,?,?,?,?)", sp)

    # Duty rosters per fixture. Stable id so a re-seed (idempotency-
    # checked above) and orchestrator paths converge on the same key.
    for fx_id, role, person_id, confirmed in DUTIES:
        did = f"d_{fx_id}_{role}"
        conn.execute(
            "INSERT INTO duties(id, fixture_id, role, person_id, "
            "confirmed) VALUES(?,?,?,?,?)",
            (did, fx_id, role, person_id, confirmed))

    # Initialise breakers as closed
    for integ in ("imap", "playhq", "fb", "twilio", "bom",
                  "council", "teamapp"):
        conn.execute(
            "INSERT OR IGNORE INTO breakers(integration, state) "
            "VALUES(?, 'closed')", (integ,))

    # Seed coach_memory rows for every coach in people. Without these,
    # reliability tracking starts empty and the match_report playbook
    # has nothing to compound against. The reports_due column is left at
    # 0 here — it'll be incremented by the schedule path when fixtures
    # complete, which is a future hook.
    for row in conn.execute(
            "SELECT id FROM people WHERE role='coach'").fetchall():
        conn.execute(
            "INSERT OR IGNORE INTO coach_memory(coach_id, reliability) "
            "VALUES(?, 1.0)", (row["id"],))

    conn.close()


if __name__ == "__main__":
    from edge.state.db import init_db
    init_db()
    seed()
    print("seeded.")
