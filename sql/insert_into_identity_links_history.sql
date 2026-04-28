INSERT INTO identity_links_history (
    system_1_email, system_1_ts,
    system_2_email, system_2_ts,
    system_3_email, system_3_ts
)
WITH base AS (
    SELECT
        ip,
        source_topic,
        email,
        ts,
        ROW_NUMBER() OVER (PARTITION BY ip, source_topic ORDER BY ts DESC) AS rn,
        MAX(ts) OVER (PARTITION BY ip) AS ip_max_ts,
        MIN(ts) OVER (PARTITION BY ip) AS ip_min_ts
    FROM login_events
    WHERE ts >= %s AND ts < %s
)
SELECT
    MAX(email) FILTER (WHERE source_topic = 'system_1') AS system_1_email,
    MAX(ts)    FILTER (WHERE source_topic = 'system_1') AS system_1_ts,
    MAX(email) FILTER (WHERE source_topic = 'system_2') AS system_2_email,
    MAX(ts)    FILTER (WHERE source_topic = 'system_2') AS system_2_ts,
    MAX(email) FILTER (WHERE source_topic = 'system_3') AS system_3_email,
    MAX(ts)    FILTER (WHERE source_topic = 'system_3') AS system_3_ts
FROM base
WHERE rn = 1
  AND ip_max_ts - ip_min_ts < INTERVAL '10 minutes'
GROUP BY ip;