import psycopg2

host = "localhost"
port = "5432"
database = "Edge_DB"
user = "postgres"
password = "1234"

create_table_query = """
CREATE TABLE sources (
    source_id SERIAL PRIMARY KEY,
    source VARCHAR(200) NOT NULL,
    source_type VARCHAR(10) NOT NULL,
    source_tag VARCHAR(10) NOT NULL,
    last_update_date TIMESTAMP NOT NULL,
    from_date TIMESTAMP NOT NULL,
    to_date TIMESTAMP NOT NULL,
    frequency VARCHAR(5) NOT NULL
);
"""

conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

cursor = conn.cursor()

cursor.execute(create_table_query)

conn.commit()

cursor.close()
conn.close()
