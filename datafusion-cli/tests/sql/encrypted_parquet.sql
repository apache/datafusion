/*
    Test parquet encryption and decryption in DataFusion SQL.
    See datafusion/common/src/config.rs for equivalent rust code
*/

-- Keys are hex encoded, you can generate these via encode, e.g.
select encode('0123456789012345', 'hex');
/*
Expected output:
+----------------------------------------------+
| encode(Utf8("0123456789012345"),Utf8("hex")) |
+----------------------------------------------+
| 30313233343536373839303132333435             |
+----------------------------------------------+
*/

CREATE EXTERNAL TABLE encrypted_parquet_table
(
double_field double,
float_field float
)
STORED AS PARQUET LOCATION 'pq/' OPTIONS (
    'format.crypto.file_encryption.encrypt_footer' 'true',
    'format.crypto.file_encryption.footer_key_as_hex' '30313233343536373839303132333435',  -- b"0123456789012345"
    'format.crypto.file_encryption.column_key_as_hex::double_field' '31323334353637383930313233343530', -- b"1234567890123450" 
    'format.crypto.file_encryption.column_key_as_hex::float_field' '31323334353637383930313233343531', -- b"1234567890123451" 
          -- Same for decryption 
    'format.crypto.file_decryption.footer_key_as_hex' '30313233343536373839303132333435', -- b"0123456789012345" 
    'format.crypto.file_decryption.column_key_as_hex::double_field' '31323334353637383930313233343530', -- b"1234567890123450" 
    'format.crypto.file_decryption.column_key_as_hex::float_field' '31323334353637383930313233343531', -- b"1234567890123451"
);

CREATE TABLE temp_table (
    double_field double,
    float_field float
);

INSERT INTO temp_table VALUES(-1.0, -1.0);
INSERT INTO temp_table VALUES(1.0, 2.0);
INSERT INTO temp_table VALUES(3.0, 4.0);
INSERT INTO temp_table VALUES(5.0, 6.0);

INSERT INTO TABLE encrypted_parquet_table(double_field, float_field) SELECT * FROM temp_table;

SELECT * FROM encrypted_parquet_table
WHERE double_field > 0.0 AND float_field > 0.0;

/*
Expected output:
+--------------+-------------+
| double_field | float_field |
+--------------+-------------+
| 1.0          | 2.0         |
| 5.0          | 6.0         |
| 3.0          | 4.0         |
+--------------+-------------+
*/

CREATE EXTERNAL TABLE parquet_table
(
double_field double,
float_field float
)
STORED AS PARQUET LOCATION 'pq/';

SELECT * FROM parquet_table;
/*
Expected output:
Parquet error: Parquet error: Parquet file has an encrypted footer but decryption properties were not provided
*/





