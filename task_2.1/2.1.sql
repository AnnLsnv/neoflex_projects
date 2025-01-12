--2.1

--Всего записей 20147
SELECT * 
FROM dm.client;

-- Уникальных 10019
SELECT * 
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY CONCAT(client_rk, effective_from_date) ORDER BY ctid) AS num, *
    FROM dm.client
) t
WHERE num = 1;

-- Дублей 10128
SELECT * 
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY CONCAT(client_rk, effective_from_date) ORDER BY ctid) AS num, *
    FROM dm.client
) t
WHERE num <> 1;

-- Чистка дублей
WITH duplicates AS (
    SELECT ctid, 
           ROW_NUMBER() OVER (PARTITION BY CONCAT(client_rk, effective_from_date) ORDER BY ctid) AS num
    FROM dm.client
)
DELETE FROM dm.client
WHERE ctid IN (
    SELECT ctid
    FROM duplicates
    WHERE num > 1
);

-- Проверка удаления
SELECT * 
FROM dm.client;