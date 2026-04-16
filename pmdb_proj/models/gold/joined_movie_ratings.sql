-- Final joined table combining cleaned IMDB basic, IMDB rating, and letterboxd tables for gold layer on movies

SELECT 
    imdb.TT_ID,
    lb.LB_ID as LB_NK,
    imdb.TITLE,
    imdb.RELEASE_YEAR,
    (imdb.RELEASE_YEAR - (imdb.RELEASE_YEAR % 10)) AS RELEASE_DECADE,
    imdb.RUNTIME_MIN,
    imdb.IMDB_RATING,
    lb.LB_RATING,
    ROUND((lb.LB_RATING * 2), 2) AS LB_ADJ_RATING,
    ROUND(LB_ADJ_RATING - imdb.IMDB_RATING, 2) AS RAW_RATING_DIFF,
    ABS(RAW_RATING_DIFF) AS ABS_RATING_DIFF,
    ROUND((LB_ADJ_RATING + imdb.IMDB_RATING) / 2, 2) AS COMPOSITE_RATING,
    imdb.NUM_VOTES,
    lb.POSTER_LINK
FROM {{ ref('imdb_clean') }} AS imdb
    INNER JOIN {{ ref('letterboxed_clean') }} AS lb
        ON imdb.TITLE = lb.TITLE
        AND imdb.RELEASE_YEAR = lb.RELEASE_YEAR
WHERE imdb.NUM_VOTES > 1000