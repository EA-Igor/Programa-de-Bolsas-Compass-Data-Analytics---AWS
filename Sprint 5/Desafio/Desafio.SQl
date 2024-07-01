COUNT(*) AS total_internacoes,
    SUM(CASE WHEN totalizador > '30' THEN 1 ELSE 0 END) AS contagem_alta
FROM S3Object
WHERE SUBSTRING(CAST(ano AS CHAR), 1, 4) = '2022' AND (mes > '5' OR mes < '10' OR UTCNOW() <> '01-10-2025')