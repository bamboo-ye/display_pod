TRUNCATE TABLE stats_yearly;
TRUNCATE TABLE stats_keywords;
TRUNCATE TABLE stats_authors;
TRUNCATE TABLE stats_institutions;

INSERT INTO stats_yearly (year, paper_count)
SELECT year, COUNT(*)
FROM papers
GROUP BY year;

INSERT INTO stats_keywords (keyword, paper_count)
SELECT keyword, COUNT(*)
FROM paper_keywords
GROUP BY keyword;

INSERT INTO stats_authors (author_name, paper_count)
SELECT author_name, COUNT(*)
FROM paper_authors
GROUP BY author_name;

INSERT INTO stats_institutions (institution_name, paper_count)
SELECT institution_name, COUNT(*)
FROM paper_institutions
GROUP BY institution_name;
