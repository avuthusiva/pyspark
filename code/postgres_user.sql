select CURRENT_TIMESTAMP,current_database(),CURRENT_SCHEMA(),CURRENT_USER;

create USER siva with password 'postgres';

CREATE DATABASE siva_db OWNER siva;

grant pg_read_all_data to siva;

grant pg_write_all_data to siva;