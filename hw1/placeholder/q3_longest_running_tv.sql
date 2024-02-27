SELECT akas.title AS TITLE, 
       (COALESCE(titles.ended, 2023) - titles.premiered) AS YEARS_RUNNING,
       titles.premiered AS P,
       titles.ended AS E
FROM titles,akas
WHERE titles.title_id = akas.title_id AND titles.premiered IS NOT NULL AND titles.type = 'tvSeries'
ORDER BY YEARS_RUNNING DESC, TITLE
LIMIT 20;