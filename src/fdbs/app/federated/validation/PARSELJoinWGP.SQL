set echo on;

alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;


/* MAX JOIN GROUP BY */
SELECT BUCHUNG.FLC, MAX(BUCHUNG.PNR) 
FROM PASSAGIER, BUCHUNG
WHERE (PASSAGIER.PNR = BUCHUNG.PNR)
GROUP BY BUCHUNG.FLC;

/* COUNT JOIN WHERE TWO COMPARISONS GROUP BY */
SELECT BUCHUNG.FLC, COUNT(*) 
FROM PASSAGIER, BUCHUNG
WHERE (PASSAGIER.PNR = BUCHUNG.PNR)
AND ((BUCHUNG.VON = 'FRA') AND (BUCHUNG.NACH = 'CDG'))
GROUP BY BUCHUNG.FLC;

/* SUM JOIN WHERE TWO COMPARISONS GROUP BY */
SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) 
FROM PASSAGIER, BUCHUNG
WHERE (PASSAGIER.PNR = BUCHUNG.PNR)
AND ((BUCHUNG.BNR = 13020) OR (BUCHUNG.BNR = 13201))
GROUP BY BUCHUNG.FLC;

/* COUNT JOIN WHERE ONE COMPARISONS GROUP BY */
SELECT BUCHUNG.FLC, COUNT(*) 
FROM PASSAGIER, BUCHUNG
WHERE (PASSAGIER.PNR = BUCHUNG.PNR)
AND ((PASSAGIER.GEBDATINT > 14000))
GROUP BY BUCHUNG.FLC;

rollback;