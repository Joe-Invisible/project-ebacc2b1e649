#!/usr/bin/env python3
"""Build the Shopping Sessions user and campaign daily Parquet tables."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

import duckdb

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000
EVENTS = ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents")
CLICKS = ("click_id", "user_id", "campaign_id", "ts")
LINKS = ("user_id_a", "user_id_b")
NORMALIZER = Path(__file__).with_name("normalize")
ENCODED_CSV = 10


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def valid_rows(path, width):
    field = rb'("[^",]*"|[^",]*)'
    row = re.compile(b",".join([field] * width))
    with path.open("rb") as source:
        next(source, None)
        for line in source:
            match = row.fullmatch(line.removesuffix(b"\n"))
            if match:
                yield tuple(value.strip(b'"') for value in match.groups())


def normalize_csv(source, target, columns):
    if NORMALIZER.is_file():
        command = [str(NORMALIZER), str(source), str(target), str(len(columns))]
        result = subprocess.run(command)
        if result.returncode == ENCODED_CSV:
            return True
        result.check_returncode()
        return False

    # Keep the starter's direct Python entry point usable without a compiler.
    encoded = False
    with target.open("wb") as output:
        output.write(",".join(columns).encode() + b"\n")
        for row in valid_rows(source, len(columns)):
            if row[0]:
                line = b",".join(row)
                if b"%" in line or b"\r" in line:
                    line = line.replace(b"%", b"%25").replace(b"\r", b"%0D")
                    encoded = True
                output.write(line + b"\n")
    return encoded


def stage_csv(con, sources, work, table, columns, workers):
    """Normalize the challenge dialect before handing rows to DuckDB."""
    sources = sorted(sources, key=lambda path: path.stat().st_size, reverse=True)
    types = {
        column: "BIGINT" if NORMALIZER.is_file() and column in ("ts", "ingest_ts") else "VARCHAR"
        for column in columns
    }
    if not sources:
        con.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {t}' for c, t in types.items())})")
        return

    targets = [work / f"{table}-{index}.csv" for index in range(len(sources))]
    with ThreadPoolExecutor(max_workers=min(workers, len(targets))) as pool:
        escaped = list(pool.map(normalize_csv, sources, targets, [columns] * len(targets)))

    schema = "{" + ", ".join(f"'{c}': '{t}'" for c, t in types.items()) + "}"
    decoded = ", ".join(
        f"replace(replace({c}, '%0D', chr(13)), '%25', '%') AS {c}" if t == "VARCHAR" else c
        for c, t in types.items()
    )
    scans = []
    for encoded in (False, True):
        paths = ", ".join(literal(path) for path, flag in zip(targets, escaped) if flag == encoded)
        if paths:
            projection = decoded if encoded else "*"
            scans.append(f"""
                SELECT {projection} FROM read_csv([{paths}], columns={schema},
                    auto_detect=false, header=true, delim=',', quote='', escape='')
            """)
    con.execute(f"CREATE VIEW {table} AS " + " UNION ALL ".join(scans))


def stage_people(con, data, work):
    """Resolve components using union by size and path compression."""
    parent = {}
    size = {}
    minimum = {}

    def root(user):
        if user not in parent:
            parent[user] = user
            size[user] = 1
            minimum[user] = user
        while user != parent[user]:
            parent[user] = parent[parent[user]]
            user = parent[user]
        return user

    for source in sorted((data / "links").glob("*.csv")):
        for first, second in valid_rows(source, len(LINKS)):
            if not first or not second:
                continue
            a, b = root(first), root(second)
            if a != b:
                if size[a] < size[b]:
                    a, b = b, a
                parent[b] = a
                size[a] += size.pop(b)
                minimum[a] = min(minimum[a], minimum.pop(b))

    # Hex protects every character in an ID from CSV syntax.
    path = work / "people.csv"
    with path.open("wb") as output:
        output.write(b"user_id,person_id\n")
        for user in parent:
            person = minimum[root(user)]
            if user != person:
                output.write(f"{user.hex()},{person.hex()}\n".encode())
    con.execute(f"""
        CREATE TABLE people AS
        SELECT decode(unhex(user_id)) AS user_id, decode(unhex(person_id)) AS person_id
        FROM read_csv({literal(path)}, columns={{'user_id':'VARCHAR', 'person_id':'VARCHAR'}},
            auto_detect=false, header=true, delim=',', quote='', escape='')
    """)


def stage_dimensions(con, data, work):
    stage_csv(con, [data / "products.csv"], work, "product_rows", ("product_id", "category"), 1)
    stage_csv(con, [data / "campaigns.csv"], work, "campaign_rows", ("campaign_id", "channel"), 1)
    con.execute("""
        CREATE TABLE products AS SELECT * FROM product_rows;
        CREATE TABLE campaigns AS SELECT * FROM campaign_rows;
    """)


def stage_sessions(con, start, end, method="baseline"):
    con.execute("""
        CREATE MACRO utc_ms(s) AS (
            epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%S'))
        );
        CREATE MACRO timestamp_ms(s) AS (
            CASE
                WHEN regexp_full_match(s, '[0-9]+') THEN try_cast(s AS BIGINT)
                WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])')
                     AND left(s, 4) <> '0000' THEN
                    utc_ms(left(s, 19)) -
                        CASE WHEN length(s) = 20 THEN 0 ELSE
                            CASE WHEN s[20] = '-' THEN -1 ELSE 1 END *
                            (cast(substr(s, 21, 2) AS BIGINT) * 60 + cast(substr(s, 24, 2) AS BIGINT)) * 60000
                        END
            END
        );
    """)
    event_time = "ts" if NORMALIZER.is_file() else "timestamp_ms(ts)"
    ingest_time = "ingest_ts" if NORMALIZER.is_file() else "utc_ms(left(ingest_ts, 19))"
    if method == "extension":
        session_query = """
            SELECT person, product_id, price,
                shopping_session_start(ts) OVER (PARTITION BY person ORDER BY ts) AS session_start
            FROM valid
        """
    else:
        boundary = f"CASE WHEN previous_ts IS NULL OR ts - previous_ts > {GAP_MS} THEN ts END"
        running_start = f"max({boundary})" if method == "baseline" else f"last_value({boundary} IGNORE NULLS)"
        # RANGE includes all peers, so every event with the same timestamp gets
        # the start found by the first peer, independent of their order.
        session_query = f"""
            SELECT person, product_id, price,
                {running_start} OVER (PARTITION BY person ORDER BY ts RANGE UNBOUNDED PRECEDING) AS session_start
            FROM (
                SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS previous_ts
                FROM valid
            )
        """
    con.execute(f"""
        CREATE TABLE session_events AS
        WITH typed AS (
            SELECT event_id, user_id, {ingest_time} AS ingest_ms,
                {event_time} AS event_ms,
                CASE WHEN product_id IS NOT NULL AND event_type IN ('view', 'cart', 'purchase')
                    THEN event_type = 'purchase' END AS is_purchase,
                CASE WHEN event_type = 'purchase' THEN product_id END AS purchased_product,
                CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                    THEN try_cast(price_cents AS BIGINT) END AS price
            FROM events
        ), dedup AS (
            SELECT arg_min(struct_pack(user_id := user_id, ts := event_ms, ingest_ms := ingest_ms,
                is_purchase := is_purchase, product_id := purchased_product, price := price), ingest_ms) AS event
            FROM typed GROUP BY event_id
        ), valid AS (
            SELECT coalesce(people.person_id, event.user_id) AS person,
                event.ts AS ts, event.product_id AS product_id, event.price AS price
            FROM dedup LEFT JOIN people ON event.user_id = people.user_id
            WHERE event.user_id IS NOT NULL AND event.ts >= ? AND event.ts < ?
                AND event.ingest_ms - event.ts <= {DAY_MS}
                AND (NOT event.is_purchase OR (event.is_purchase AND event.price BETWEEN 1 AND 10000000))
        )
        {session_query}
    """, [start, end])
    con.execute("""
        CREATE TABLE sessions AS
        SELECT person, session_start, cast(make_timestamp_ms(session_start) AS DATE) AS day,
            count(*) AS events, coalesce(sum(price), 0)::BIGINT AS revenue
        FROM session_events
        GROUP BY person, session_start
    """)


def stage_clicks(con, start, end):
    # Identical click copies have no effect on this minimum or on assist eligibility.
    click_time = "ts" if NORMALIZER.is_file() else "timestamp_ms(ts)"
    con.execute(f"""
        CREATE TABLE valid_clicks AS
        WITH typed AS (
            SELECT click_id, user_id, campaign_id, {click_time} AS click_ts
            FROM clicks WHERE user_id IS NOT NULL
        )
        SELECT coalesce(people.person_id, typed.user_id) AS person, campaign_id, click_ts,
            min(click_id) AS click_id
        FROM typed JOIN campaigns USING (campaign_id) LEFT JOIN people USING (user_id)
        WHERE click_ts >= ? AND click_ts < ?
        GROUP BY person, campaign_id, click_ts
    """, [start - WEEK_MS, end])


def user_daily(con):
    return con.sql("""
        WITH daily AS (
            SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
                sum(revenue)::BIGINT AS revenue_cents
            FROM sessions GROUP BY person, day
        ), category_totals AS (
            SELECT person, cast(make_timestamp_ms(session_start) AS DATE) AS day,
                coalesce(category, 'unknown') AS category, sum(price) AS revenue
            FROM session_events LEFT JOIN products USING (product_id)
            WHERE price IS NOT NULL
            GROUP BY person, day, coalesce(category, 'unknown')
        ), top_categories AS (
            SELECT person, day, arg_min(category, row(-revenue, category)) AS top_category
            FROM category_totals GROUP BY person, day
        )
        SELECT person AS user_id, day, sessions, events, revenue_cents, top_category
        FROM daily LEFT JOIN top_categories USING (person, day)
    """)


def campaign_daily(con):
    # Each click covers the next seven days. Merge overlapping coverage so a
    # campaign can match a session at most once and repeated clicks stay cheap.
    return con.sql(f"""
        WITH click_winners AS (
            SELECT person, click_ts, arg_min(campaign_id, click_id) AS campaign_id
            FROM valid_clicks GROUP BY person, click_ts
        ), attributed AS (
            SELECT CASE WHEN session_start - click_ts <= {WEEK_MS}
                THEN campaign_id ELSE 'organic' END AS campaign_id, day, revenue
            FROM sessions ASOF LEFT JOIN click_winners
                ON sessions.person = click_winners.person AND session_start >= click_ts
        ), credited AS (
            SELECT campaign_id, day, count(*) AS sessions,
                count(*) FILTER (WHERE revenue > 0) AS purchasing_sessions,
                sum(revenue)::BIGINT AS revenue_cents
            FROM attributed GROUP BY campaign_id, day
        ), click_gaps AS (
            SELECT *, lag(click_ts) OVER (PARTITION BY person, campaign_id ORDER BY click_ts) AS previous_ts
            FROM valid_clicks
        ), interval_members AS (
            SELECT *, max(CASE WHEN previous_ts IS NULL OR click_ts - previous_ts > {WEEK_MS} + 1
                              THEN click_ts END)
                OVER (PARTITION BY person, campaign_id ORDER BY click_ts ROWS UNBOUNDED PRECEDING) AS first_ts
            FROM click_gaps
        ), intervals AS (
            SELECT person, campaign_id, first_ts, max(click_ts) + {WEEK_MS} AS last_ts
            FROM interval_members GROUP BY person, campaign_id, first_ts
        ), assists AS (
            SELECT campaign_id, day, count(*) AS assisted_sessions
            FROM sessions s JOIN intervals i ON s.person = i.person
                AND s.session_start BETWEEN i.first_ts AND i.last_ts
            GROUP BY campaign_id, day
        )
        SELECT campaign_id, CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE channel END AS channel,
            day, coalesce(sessions, 0) AS sessions, coalesce(purchasing_sessions, 0) AS purchasing_sessions,
            coalesce(revenue_cents, 0) AS revenue_cents, coalesce(assisted_sessions, 0) AS assisted_sessions
        FROM credited FULL JOIN assists USING (campaign_id, day)
        LEFT JOIN campaigns USING (campaign_id)
    """)


def main(session_method="baseline", extension=None):
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    ncpu = int(os.environ.get("NCPU", len(os.sched_getaffinity(0))))
    memory = max(1, int(int(os.environ.get("MEM_GB", "8")) * 0.8))
    manifest = json.loads((data / "manifest.json").read_text())
    scratch.mkdir(parents=True, exist_ok=True)

    config = {"allow_unsigned_extensions": True} if extension else {}
    with tempfile.TemporaryDirectory(prefix="shopping-", dir=scratch) as temp, duckdb.connect(config=config) as con:
        if extension:
            con.execute(f"LOAD {literal(extension)}")
        work = Path(temp)
        con.execute(f"SET threads = {ncpu}")
        con.execute(f"SET memory_limit = '{memory}GB'")
        con.execute(f"SET temp_directory = {literal(work / 'spill')}")
        con.execute("SET preserve_insertion_order = false")
        start, end = con.execute(
            "SELECT epoch_ms(?::TIMESTAMP), epoch_ms(?::TIMESTAMP)",
            [manifest["window_start"], manifest["window_end"]],
        ).fetchone()
        stage_dimensions(con, data, work)
        stage_people(con, data, work)
        stage_csv(con, (data / "events").glob("*.csv"), work, "events", EVENTS, ncpu)
        stage_sessions(con, start, end, session_method)
        stage_csv(con, (data / "clicks").glob("*.csv"), work, "clicks", CLICKS, ncpu)
        stage_clicks(con, start, end)

        for name, results in (("user_daily", user_daily(con)), ("campaign_daily", campaign_daily(con))):
            destination = output / name
            destination.mkdir(parents=True, exist_ok=True)
            results.write_parquet(str(destination / "part-0.parquet"))


if __name__ == "__main__":
    main()
