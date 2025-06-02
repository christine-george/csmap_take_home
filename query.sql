SELECT DISTINCT e.title AS episode_title, 
                p.title AS podcast_title, 
                s.text AS segment_text
FROM information.episode e
JOIN information.show p 
    ON e.title_detail->>'base' = p.title_detail->>'base'
JOIN transcript.segmented s
    ON e.id = s.id
WHERE e.published BETWEEN ('2024-11-05'::date - INTERVAL '14 days') 
      AND ('2024-11-05'::date + INTERVAL '14 days')
  AND (s.text ~* '\mTrump\M' OR s.text ~* '\mBiden\M');
