USE hbase;

SELECT
CONVERT_FROM(Bozorth3.Bozorth3.probeId, 'UTF8') probe,
CONVERT_FROM(Bozorth3.Bozorth3.galleryId, 'UTF8') gallery,
CONVERT_FROM(Bozorth3.Bozorth3.score, 'INT_BE') score
FROM Bozorth3
ORDER BY score
DESC
LIMIT 10
;
