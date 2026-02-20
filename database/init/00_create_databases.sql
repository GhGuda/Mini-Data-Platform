-- Create Airflow metadata database
CREATE DATABASE airflow_db;

-- Ensure correct owner
ALTER DATABASE airflow_db OWNER TO mini_user;