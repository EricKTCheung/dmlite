--
--  Copyright (C) 2004-2011 by CERN/IT/GD/CT
--  All rights reserved
--
--       @(#)$RCSfile: dpm_mysql_tbl.sql,v $ $Revision: 7068 $ $Date: 2012-08-30 12:04:36 +0200 (Thu, 30 Aug 2012) $ CERN IT-GD/CT Jean-Philippe Baud
 
--     Create Disk Pool Manager MySQL tables.

CREATE DATABASE dpm_db;
USE dpm_db;
CREATE TABLE dpm_pool (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       poolname VARCHAR(15) BINARY,
       defsize BIGINT UNSIGNED,
       gc_start_thresh INTEGER,
       gc_stop_thresh INTEGER,
       def_lifetime INTEGER,
       defpintime INTEGER,
       max_lifetime INTEGER,
       maxpintime INTEGER,
       fss_policy VARCHAR(15) BINARY,
       gc_policy VARCHAR(15) BINARY,
       mig_policy VARCHAR(15) BINARY,
       rs_policy VARCHAR(15) BINARY,
       groups VARCHAR(255) BINARY,
       ret_policy CHAR(1),
       s_type CHAR(1),
       pooltype VARCHAR(32),
       poolmeta TEXT)
	ENGINE = InnoDB;

CREATE TABLE dpm_fs (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       poolname VARCHAR(15) BINARY,
       server VARCHAR(63) BINARY,
       fs VARCHAR(79) BINARY,
       status INTEGER,
       weight INTEGER)
	ENGINE = InnoDB;

CREATE TABLE dpm_pending_req (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       r_ordinal INTEGER,
       r_token VARCHAR(36) BINARY,
       r_uid INTEGER,
       r_gid INTEGER,
       client_dn VARCHAR(255) BINARY,
       clienthost VARCHAR(63) BINARY,
       r_type CHAR(1) BINARY,
       u_token VARCHAR(255) BINARY,
       flags INTEGER,
       retrytime INTEGER,
       nbreqfiles INTEGER,
       ctime INTEGER,
       stime INTEGER,
       etime INTEGER,
       status INTEGER,
       errstring VARCHAR(255) BINARY,
       groups VARCHAR(255))
	ENGINE = InnoDB;

CREATE TABLE dpm_req (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       r_ordinal INTEGER,
       r_token VARCHAR(36) BINARY,
       r_uid INTEGER,
       r_gid INTEGER,
       client_dn VARCHAR(255) BINARY,
       clienthost VARCHAR(63) BINARY,
       r_type CHAR(1) BINARY,
       u_token VARCHAR(255) BINARY,
       flags INTEGER,
       retrytime INTEGER,
       nbreqfiles INTEGER,
       ctime INTEGER,
       stime INTEGER,
       etime INTEGER,
       status INTEGER,
       errstring VARCHAR(255) BINARY,
       groups VARCHAR(255))
	ENGINE = InnoDB;

CREATE TABLE dpm_get_filereq (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       r_token VARCHAR(36) BINARY,
       f_ordinal INTEGER,
       r_uid INTEGER,
       from_surl BLOB,
       protocol VARCHAR(7) BINARY,
       lifetime INTEGER,
       f_type CHAR(1) BINARY,
       s_token CHAR(36) BINARY,
       ret_policy CHAR(1),
       flags INTEGER,
       server VARCHAR(63) BINARY,
       pfn BLOB,
       actual_size BIGINT UNSIGNED,
       status INTEGER,
       errstring VARCHAR(255) BINARY)
	ENGINE = InnoDB;

CREATE TABLE dpm_put_filereq (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       r_token VARCHAR(36) BINARY,
       f_ordinal INTEGER,
       to_surl BLOB,
       protocol VARCHAR(7) BINARY,
       lifetime INTEGER,
       f_lifetime INTEGER,
       f_type CHAR(1) BINARY,
       s_token CHAR(36) BINARY,
       ret_policy CHAR(1),
       ac_latency CHAR(1),
       requested_size BIGINT UNSIGNED,
       server VARCHAR(63) BINARY,
       pfn BLOB,
       actual_size BIGINT UNSIGNED,
       status INTEGER,
       errstring VARCHAR(255) BINARY)
	ENGINE = InnoDB;

CREATE TABLE dpm_copy_filereq (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       r_token VARCHAR(36) BINARY,
       f_ordinal INTEGER,
       from_surl BLOB,
       to_surl BLOB,
       f_lifetime INTEGER,
       f_type CHAR(1) BINARY,
       s_token CHAR(36) BINARY,
       ret_policy CHAR(1),
       ac_latency CHAR(1),
       flags INTEGER,
       actual_size BIGINT UNSIGNED,
       status INTEGER,
       errstring VARCHAR(255) BINARY)
	ENGINE = InnoDB;

CREATE TABLE dpm_space_reserv (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       s_token CHAR(36) BINARY,
       client_dn VARCHAR(255) BINARY,
       s_uid INTEGER,
       s_gid INTEGER,
       ret_policy CHAR(1),
       ac_latency CHAR(1),
       s_type CHAR(1) BINARY,
       u_token VARCHAR(255) BINARY,
       t_space BIGINT UNSIGNED,
       g_space BIGINT UNSIGNED,
       u_space BIGINT,
       poolname VARCHAR(15) BINARY,
       assign_time INTEGER,
       expire_time INTEGER,
       groups VARCHAR(255) BINARY,
       path VARCHAR(255) BINARY)
	ENGINE = InnoDB;

CREATE TABLE dpm_unique_id (
       id BIGINT UNSIGNED)
	ENGINE = InnoDB;

ALTER TABLE dpm_pool
       ADD UNIQUE (poolname);
ALTER TABLE dpm_fs
       ADD UNIQUE pk_fs (poolname, server, fs);
ALTER TABLE dpm_pending_req
       ADD UNIQUE (r_token);
ALTER TABLE dpm_req
       ADD UNIQUE (r_token);
ALTER TABLE dpm_get_filereq
       ADD UNIQUE pk_g_fullid (r_token, f_ordinal);
ALTER TABLE dpm_put_filereq
       ADD UNIQUE pk_p_fullid (r_token, f_ordinal);
ALTER TABLE dpm_copy_filereq
       ADD UNIQUE pk_c_fullid (r_token, f_ordinal);
ALTER TABLE dpm_space_reserv
       ADD UNIQUE (s_token);
ALTER TABLE dpm_space_reserv
       ADD UNIQUE (path);
ALTER TABLE dpm_fs
       ADD CONSTRAINT fk_fs FOREIGN KEY (poolname) REFERENCES dpm_pool(poolname);

CREATE INDEX P_U_DESC_IDX ON dpm_pending_req(u_token);
CREATE INDEX U_DESC_IDX ON dpm_req(u_token);
CREATE INDEX G_SURL_IDX ON dpm_get_filereq(FROM_SURL(255));
CREATE INDEX P_SURL_IDX ON dpm_put_filereq(TO_SURL(255));
CREATE INDEX CF_SURL_IDX ON dpm_copy_filereq(FROM_SURL(255));    
CREATE INDEX CT_SURL_IDX ON dpm_copy_filereq(TO_SURL(255));
CREATE INDEX G_PFN_IDX ON dpm_get_filereq(pfn(255));
CREATE INDEX P_PFN_IDX ON dpm_put_filereq(pfn(255));

-- Create the "schema_version" table

DROP TABLE IF EXISTS schema_version_dpm;
CREATE TABLE schema_version_dpm (
  major INTEGER NOT NULL,
  minor INTEGER NOT NULL,
  patch INTEGER NOT NULL
) ENGINE=INNODB;

INSERT INTO schema_version_dpm (major, minor, patch)
  VALUES (3, 5, 0);
