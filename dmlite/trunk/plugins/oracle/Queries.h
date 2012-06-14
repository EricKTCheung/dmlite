/// @file    plugins/oracle/Queries.h
/// @brief   Oracle queries.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef QUERIES_H
#define	QUERIES_H

#include <dmlite/dm_exceptions.h>
#include <occi.h>

enum {
  STMT_GET_FILE_BY_ID = 0,
  STMT_GET_FILE_BY_GUID,
  STMT_GET_FILE_BY_NAME,
  STMT_GET_LIST_FILES,
  STMT_GET_SYMLINK,
  STMT_GET_USERINFO_BY_NAME,
  STMT_GET_USERINFO_BY_UID,
  STMT_GET_GROUPINFO_BY_NAME,
  STMT_GET_GROUPINFO_BY_GID,
  STMT_GET_FILE_REPLICAS,
  STMT_GET_REPLICA_BY_URL,
  STMT_GET_COMMENT,
  STMT_SET_GUID,
  STMT_SET_COMMENT,
  STMT_INSERT_COMMENT,
  STMT_INSERT_FILE,
  STMT_INSERT_SYMLINK,
  STMT_DELETE_FILE,
  STMT_DELETE_COMMENT,
  STMT_DELETE_SYMLINK,
  STMT_SELECT_NLINK_FOR_UPDATE,
  STMT_UPDATE_NLINK,
  STMT_UPDATE_PERMS,
  STMT_DELETE_REPLICA,
  STMT_ADD_REPLICA,
  STMT_TRUNCATE_FILE,
  STMT_GET_UNIQ_UID_FOR_UPDATE,
  STMT_GET_UNIQ_GID_FOR_UPDATE,
  STMT_UPDATE_UNIQ_UID,
  STMT_UPDATE_UNIQ_GID,
  STMT_INSERT_UNIQ_UID,
  STMT_INSERT_UNIQ_GID,
  STMT_INSERT_USER,
  STMT_INSERT_GROUP,
  STMT_CHANGE_NAME,
  STMT_CHANGE_PARENT,
  STMT_UTIME,
  STMT_UPDATE_REPLICA,
  STMT_SENTINEL
};

/// Used internally to define prepared statements.
/// Must match with STMT_* constants!
static const char* statements[] = {
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE fileid = :b_fileid",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE guid = :b_guid",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = :b_parent AND name = :b_name",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = :b_parent",
  "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = :b_fileid",
  "SELECT userid, username, user_ca, NVL(banned, 0)\
        FROM Cns_userinfo\
        WHERE username = :b_username",
  "SELECT userid, username, user_ca, NVL(banned, 0)\
        FROM Cns_userinfo\
        WHERE userid = :b_uid",
  "SELECT gid, groupname, NVL(banned, 0)\
        FROM Cns_groupinfo\
        WHERE groupname = :b_groupname",
  "SELECT gid, groupname, NVL(banned, 0)\
        FROM Cns_groupinfo\
        WHERE gid = :b_gid",
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, nbaccesses, atime, ptime, ltime, status, f_type, poolname, host, fs, sfn\
        FROM Cns_file_replica\
        WHERE fileid = :b_fileid",
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, nbaccesses, atime, ptime, ltime, status, f_type, poolname, host, fs, sfn\
        FROM Cns_file_replica\
        WHERE sfn = :b_sfn",
  "SELECT comments\
        FROM Cns_user_metadata\
        WHERE u_fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET guid = :b_guid\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_user_metadata\
        SET comments = :b_comment\
        WHERE u_fileid = :b_fileid",
  "INSERT INTO Cns_user_metadata\
          (u_fileid, comments)\
        VALUES\
          (:b_fileid, :b_comment)",
  "INSERT INTO Cns_file_metadata\
          (fileid, parent_fileid, name, filemode, nlink, owner_uid, gid,\
           filesize, atime, mtime, ctime, fileclass, status,\
           csumtype, csumvalue, acl)\
        VALUES\
          (Cns_unique_id.nextval, :b_parentid, :b_name, :b_mode, :b_nlink, :b_uid, :b_gid,\
           :b_size, :b_atime, :b_mtime, :b_ctime, :b_fclass, :b_status,\
           :b_sumtype, :b_sumvalue, :b_acl)",
  "INSERT INTO Cns_symlinks\
          (fileid, linkname)\
        VALUES\
          (:b_fileid, :b_linkname)",
  "DELETE FROM Cns_file_metadata WHERE fileid = :b_fileid",
  "DELETE FROM Cns_user_metadata WHERE u_fileid = :b_fileid",
  "DELETE FROM Cns_symlinks WHERE fileid = :b_fileid",
  "SELECT nlink FROM Cns_file_metadata WHERE fileid = :b_fileid FOR UPDATE",
  "UPDATE Cns_file_metadata\
        SET nlink = :b_nlink, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET owner_uid = :b_uid, gid = :b_gid, filemode = :b_mode, acl = :b_acl, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "DELETE FROM Cns_file_replica\
        WHERE fileid = :b_fileid AND sfn = :b_sfn",
  "INSERT INTO Cns_file_replica\
          (fileid, nbaccesses,\
           ctime, atime, ptime, ltime,\
           r_type, status, f_type,\
           setname, poolname, host, fs, sfn)\
        VALUES\
          (:b_fileid, 0,\
           :b_ctime, :b_atime, :b_ptime, :b_ltime,\
           :b_rtype, :b_status, :b_ftype,\
           :b_setname, :b_pool, :b_host, :b_fs, :b_sfn)",
  "UPDATE Cns_file_metadata\
        SET filesize = 0, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "SELECT id FROM Cns_unique_uid FOR UPDATE",
  "SELECT id FROM Cns_unique_gid FOR UPDATE",
  "UPDATE Cns_unique_uid SET id = :b_uid",
  "UPDATE Cns_unique_gid SET id = :b_gid",
  "INSERT INTO Cns_unique_uid (id) VALUES (:b_uid)",
  "INSERT INTO Cns_unique_gid (id) VALUES (:b_gid)",
  "INSERT INTO Cns_userinfo\
          (userid, username, user_ca, banned)\
        VALUES\
          (:b_uid, :b_username, :b_ca, :b_banned)",
  "INSERT INTO Cns_groupinfo\
          (gid, groupname, banned)\
        VALUES\
          (:b_gid, :b_groupname, :b_banned)",
  "UPDATE Cns_file_metadata\
        SET name = :b_name, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET parent_fileid = :b_parent, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET atime = :b_atime, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_replica\
        SET atime = :b_atime, ltime = :b_ltime, nbaccesses = :b_nacc, status = :b_status, f_type = :b_type\
        WHERE sfn = :b_sfn"
};



inline oracle::occi::Statement* getPreparedStatement(oracle::occi::Connection* conn, unsigned stId)
{
  oracle::occi::Statement* stmt;

  try {
    stmt = conn->createStatement(statements[stId]);
    // Make sure autocommit is always disabled
    stmt->setAutoCommit(false);
  }
  catch (oracle::occi::SQLException& ex) {
    throw dmlite::DmException(DM_QUERY_FAILED, std::string("Prepare: ") + ex.getMessage());
  }
  return stmt;
}

#endif // QUERIES_H
