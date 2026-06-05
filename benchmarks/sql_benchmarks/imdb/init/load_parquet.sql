CREATE EXTERNAL TABLE aka_name (
    id integer unsigned NOT NULL,
    person_id integer NOT NULL,
    name varchar(218) NOT NULL,
    imdb_index varchar(12),
    name_pcode_cf varchar(5),
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/aka_name.parquet';

CREATE EXTERNAL TABLE aka_title (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    title varchar(553) NOT NULL,
    imdb_index varchar(12),
    kind_id integer NOT NULL,
    production_year integer,
    phonetic_code varchar(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    note varchar(72),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/aka_title.parquet';

CREATE EXTERNAL TABLE cast_info (
    id integer unsigned NOT NULL,
    person_id integer NOT NULL,
    movie_id integer NOT NULL,
    person_role_id integer,
    note varchar(992),
    nr_order integer,
    role_id integer NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/cast_info.parquet';

CREATE EXTERNAL TABLE char_name (
    id integer unsigned NOT NULL,
    name varchar(478) NOT NULL,
    imdb_index varchar(12),
    imdb_id integer,
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/char_name.parquet';

CREATE EXTERNAL TABLE comp_cast_type (
    id integer unsigned NOT NULL,
    kind varchar(32) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/comp_cast_type.parquet';

CREATE EXTERNAL TABLE company_name (
    id integer unsigned NOT NULL,
    name varchar(200) NOT NULL,
    country_code varchar(255),
    imdb_id integer,
    name_pcode_nf varchar(5),
    name_pcode_sf varchar(5),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/company_name.parquet';

CREATE EXTERNAL TABLE company_type (
    id integer unsigned NOT NULL,
    kind varchar(32) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/company_type.parquet';

CREATE EXTERNAL TABLE complete_cast (
    id integer unsigned NOT NULL,
    movie_id integer,
    subject_id integer NOT NULL,
    status_id integer NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/complete_cast.parquet';

CREATE EXTERNAL TABLE info_type (
    id integer unsigned NOT NULL,
    info varchar(32) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/info_type.parquet';

CREATE EXTERNAL TABLE keyword (
    id integer unsigned NOT NULL,
    keyword varchar(74) NOT NULL,
    phonetic_code varchar(5)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/keyword.parquet';

CREATE EXTERNAL TABLE kind_type (
    id integer unsigned NOT NULL,
    kind varchar(15) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/kind_type.parquet';

CREATE EXTERNAL TABLE link_type (
    id integer unsigned NOT NULL,
    link varchar(32) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/link_type.parquet';

CREATE EXTERNAL TABLE movie_companies (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    company_id integer NOT NULL,
    company_type_id integer NOT NULL,
    note varchar(208)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/movie_companies.parquet';

CREATE EXTERNAL TABLE movie_info (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info varchar(8000) NOT NULL,
    note varchar(387)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/movie_info.parquet';

CREATE EXTERNAL TABLE movie_info_idx (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info varchar(10) NOT NULL,
    note varchar(1)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/movie_info_idx.parquet';

CREATE EXTERNAL TABLE movie_keyword (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    keyword_id integer NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/movie_keyword.parquet';

CREATE EXTERNAL TABLE movie_link (
    id integer unsigned NOT NULL,
    movie_id integer NOT NULL,
    linked_movie_id integer NOT NULL,
    link_type_id integer NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/movie_link.parquet';

CREATE EXTERNAL TABLE name (
    id integer unsigned NOT NULL,
    name varchar(106) NOT NULL,
    imdb_index varchar(12),
    imdb_id integer,
    gender varchar(1),
    name_pcode_cf varchar(5),
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/name.parquet';

CREATE EXTERNAL TABLE person_info (
    id integer unsigned NOT NULL,
    person_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note varchar(430)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/person_info.parquet';

CREATE EXTERNAL TABLE role_type (
    id integer unsigned NOT NULL,
    role varchar(32) NOT NULL
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/role_type.parquet';

CREATE EXTERNAL TABLE title (
    id integer unsigned NOT NULL,
    title varchar(334) NOT NULL,
    imdb_index varchar(12),
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code varchar(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years varchar(49),
    md5sum varchar(32)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/imdb/title.parquet';
