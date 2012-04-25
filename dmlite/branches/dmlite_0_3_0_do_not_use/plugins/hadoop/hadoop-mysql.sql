use dpm_db;

CREATE TABLE IF NOT EXISTS hadoop_pools
  (poolname VARCHAR(15) PRIMARY KEY,
   hostname VARCHAR(255) NOT NULL,
   port     INT NOT NULL,
   username VARCHAR(64));
