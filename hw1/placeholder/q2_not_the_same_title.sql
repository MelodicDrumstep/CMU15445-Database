SELECT premiered, primary_title || '(' || original_title || ')' AS "primary_title(original_title)"
FROM titles
WHERE titles.genres LIKE "%Action%" and primary_title != original_title
ORDER BY premiered DESC
LIMIT 10