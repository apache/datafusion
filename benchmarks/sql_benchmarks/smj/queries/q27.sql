-- Keep the newest of 128 versions of each of 1024 documents.
WITH documents AS (
    SELECT value / 128 AS document_id, value % 128 AS version
    FROM range(131072)
)
SELECT d.document_id, d.version
FROM documents d
WHERE NOT EXISTS (
    SELECT 1 FROM documents newer
    WHERE newer.document_id = d.document_id AND newer.version > d.version
)
