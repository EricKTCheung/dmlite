--
--  Copyright (C) 2001-2010 by CERN/IT/DS/HSM
--  All rights reserved
--
--       @(#)$RCSfile: Cns_mysql_tbl.sql,v $ $Revision: 7068 $ $Date: 2012-08-30 12:04:36 +0200 (Thu, 30 Aug 2012) $ CERN IT-DS/HSM Jean-Philippe Baud
 
--     Create name server MySQL tables.

CREATE DATABASE cns_db;
USE cns_db;
CREATE TABLE Cns_class_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       classid INTEGER,
       name VARCHAR(15) BINARY,
       owner_uid INTEGER,
       gid INTEGER,
       min_filesize INTEGER,
       max_filesize INTEGER,
       flags INTEGER,
       maxdrives INTEGER,
       max_segsize INTEGER,
       migr_time_interval INTEGER,
       mintime_beforemigr INTEGER,
       nbcopies INTEGER,
       nbdirs_using_class INTEGER,
       retenp_on_disk INTEGER)
	ENGINE = InnoDB;

CREATE TABLE Cns_file_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       parent_fileid BIGINT UNSIGNED,
       guid CHAR(36) BINARY,
       name VARCHAR(255) BINARY,
       filemode INTEGER UNSIGNED,
       nlink INTEGER,
       owner_uid INTEGER UNSIGNED,
       gid INTEGER UNSIGNED,
       filesize BIGINT UNSIGNED,
       atime INTEGER,
       mtime INTEGER,
       ctime INTEGER,
       fileclass SMALLINT,
       status CHAR(1) BINARY,
       csumtype VARCHAR(2) BINARY,
       csumvalue VARCHAR(32) BINARY,
       acl BLOB,
       xattr TEXT)
	ENGINE = InnoDB;

CREATE TABLE Cns_user_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       u_fileid BIGINT UNSIGNED,
       comments VARCHAR(255) BINARY)
	ENGINE = InnoDB;

CREATE TABLE Cns_symlinks (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       linkname BLOB)
	ENGINE = InnoDB;

CREATE TABLE Cns_file_replica (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       nbaccesses BIGINT UNSIGNED,
       ctime INTEGER,
       atime INTEGER,
       ptime INTEGER,
       ltime INTEGER,
       r_type CHAR(1) BINARY,
       status CHAR(1) BINARY,
       f_type CHAR(1) BINARY,
       setname VARCHAR(36) BINARY,
       poolname VARCHAR(15) BINARY,
       host VARCHAR(63) BINARY,
       fs VARCHAR(79) BINARY,
       sfn BLOB,
       xattr TEXT)
	ENGINE = InnoDB;

CREATE TABLE Cns_groupinfo (
       rowid INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       gid INTEGER,
       groupname VARCHAR(255) BINARY,
       banned INTEGER,
       xattr TEXT)
	ENGINE = InnoDB;

CREATE TABLE Cns_userinfo (
       rowid INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       userid INTEGER,
       username VARCHAR(255) BINARY,
       user_ca VARCHAR(255) BINARY,
       banned INTEGER,
       xattr TEXT)
	ENGINE = InnoDB;

CREATE TABLE Cns_unique_id (
       id BIGINT UNSIGNED)
	ENGINE = InnoDB;

CREATE TABLE Cns_unique_gid (
       id INTEGER UNSIGNED)
	ENGINE = InnoDB;

CREATE TABLE Cns_unique_uid (
       id INTEGER UNSIGNED)
	ENGINE = InnoDB;

ALTER TABLE Cns_class_metadata
       ADD UNIQUE (classid),
       ADD UNIQUE classname (name);
ALTER TABLE Cns_file_metadata
       ADD UNIQUE (fileid),
       ADD UNIQUE file_full_id (parent_fileid, name),
       ADD UNIQUE (guid);
ALTER TABLE Cns_user_metadata
       ADD UNIQUE (u_fileid);
ALTER TABLE Cns_symlinks
       ADD UNIQUE (fileid);
ALTER TABLE Cns_file_replica
       ADD INDEX (fileid),
       ADD INDEX (host),
       ADD INDEX (sfn(255));
ALTER TABLE Cns_groupinfo
       ADD UNIQUE (groupname);
ALTER TABLE Cns_userinfo
       ADD UNIQUE (username);

ALTER TABLE Cns_user_metadata
       ADD CONSTRAINT fk_u_fileid FOREIGN KEY (u_fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_symlinks
       ADD CONSTRAINT fk_l_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_file_replica
       ADD CONSTRAINT fk_r_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);

CREATE INDEX PARENT_FILEID_IDX ON Cns_file_metadata (PARENT_FILEID);
CREATE INDEX linkname_idx ON Cns_symlinks(linkname(255));

-- Create the "schema_version" table

DROP TABLE IF EXISTS schema_version;
CREATE TABLE schema_version (
  major INTEGER NOT NULL,
  minor INTEGER NOT NULL,
  patch INTEGER NOT NULL
) ENGINE=INNODB;

INSERT INTO schema_version (major, minor, patch) 
  VALUES (3, 2, 0);
