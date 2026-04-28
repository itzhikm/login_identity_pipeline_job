MERGE INTO identity_links AS t
USING (
    SELECT DISTINCT
        system_1_email,
        system_2_email,
        system_3_email
    FROM identity_links_history
    WHERE COALESCE(system_1_ts, system_2_ts, system_3_ts) >= %s
      AND COALESCE(system_1_ts, system_2_ts, system_3_ts) <  %s
) AS s
ON  t.system_1_email = s.system_1_email
 OR t.system_2_email = s.system_2_email
 OR t.system_3_email = s.system_3_email
WHEN MATCHED THEN
    UPDATE SET
        system_1_email = COALESCE(t.system_1_email, s.system_1_email),
        system_2_email = COALESCE(t.system_2_email, s.system_2_email),
        system_3_email = COALESCE(t.system_3_email, s.system_3_email),
        last_updated   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (system_1_email, system_2_email, system_3_email, last_updated)
    VALUES (s.system_1_email, s.system_2_email, s.system_3_email, CURRENT_TIMESTAMP);