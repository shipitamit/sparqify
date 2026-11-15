SELECT id, name::text, created_at::date, COALESCE(score,0)::int AS s, STRING_AGG(tag, ',') AS tags
FROM events WHERE created_at > NOW() - INTERVAL '7 days' GROUP BY id,name,created_at,score LIMIT 20;
