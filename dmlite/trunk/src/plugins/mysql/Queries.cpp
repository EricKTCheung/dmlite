/// @file   Queries.cpp
/// @brief  MYSQL queries.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>

// Catalog

const char* STMT_GET_FILE_BY_ID =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl, xattr\
        FROM Cns_file_metadata\
        WHERE fileid = ?";
const char* STMT_GET_FILE_BY_GUID =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl, xattr\
        FROM Cns_file_metadata\
        WHERE guid = ?";
const char* STMT_GET_FILE_BY_NAME =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl, xattr\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ? AND name = ?";
const char* STMT_GET_LIST_FILES =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl, xattr\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ?\
        ORDER BY name ASC";
const char* STMT_GET_SYMLINK =
    "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = ?";
const char* STMT_GET_FILE_REPLICAS =
    "SELECT rowid, fileid, nbaccesses,\
            atime, ptime, ltime,\
            status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
        FROM Cns_file_replica\
        WHERE fileid = ?";
const char* STMT_GET_REPLICA_BY_ID =
    "SELECT rowid, fileid, nbaccesses,\
            atime, ptime, ltime,\
            status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
        FROM Cns_file_replica\
        WHERE rowid = ?";
const char* STMT_GET_REPLICA_BY_URL =
    "SELECT rowid, fileid, nbaccesses,\
            atime, ptime, ltime,\
            status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
        FROM Cns_file_replica\
        WHERE sfn = ?";
const char* STMT_GET_COMMENT =
    "SELECT comments\
        FROM Cns_user_metadata\
        WHERE u_fileid = ?";
const char* STMT_SET_GUID =
    "UPDATE Cns_file_metadata\
        SET guid = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_SET_XATTR =
    "UPDATE Cns_file_metadata\
        SET xattr = ?\
        WHERE fileid = ?";
const char* STMT_SET_COMMENT =
    "UPDATE Cns_user_metadata\
        SET comments = ?\
        WHERE u_fileid = ?";
const char* STMT_INSERT_COMMENT =
    "INSERT INTO Cns_user_metadata\
          (u_fileid, comments)\
        VALUES\
          (?, ?)";
const char* STMT_INSERT_FILE =
    "INSERT INTO Cns_file_metadata\
          (fileid, parent_fileid, name, filemode, nlink, owner_uid, gid,\
           filesize, atime, mtime, ctime, fileclass, status,\
           csumtype, csumvalue, acl, xattr)\
        VALUES\
          (?, ?, ?, ?, ?, ?, ?,\
           ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), ?, ?,\
           ?, ?, ?, ?)";
const char* STMT_INSERT_SYMLINK =
    "INSERT INTO Cns_symlinks\
          (fileid, linkname)\
        VALUES\
          (?, ?)";
const char* STMT_SELECT_UNIQ_ID_FOR_UPDATE =
    "SELECT id FROM Cns_unique_id FOR UPDATE";
const char* STMT_UPDATE_UNIQ_ID =
    "UPDATE Cns_unique_id SET id = ?";
const char* STMT_INSERT_UNIQ_ID =
    "INSERT INTO Cns_unique_id (id) VALUES (?)";
const char* STMT_DELETE_FILE =
    "DELETE FROM Cns_file_metadata WHERE fileid = ?";
const char* STMT_DELETE_COMMENT =
    "DELETE FROM Cns_user_metadata WHERE u_fileid = ?";
const char* STMT_DELETE_SYMLINK =
    "DELETE FROM Cns_symlinks WHERE fileid = ?";
const char* STMT_NLINK_FOR_UPDATE =
    "SELECT nlink FROM Cns_file_metadata WHERE fileid = ? FOR UPDATE";
const char* STMT_UPDATE_NLINK =
    "UPDATE Cns_file_metadata\
        SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_UPDATE_PERMS =
    "UPDATE Cns_file_metadata\
        SET owner_uid = if(? = -1, owner_uid, ?),\
            gid = if(? = -1, gid, ?),\
            filemode = ((filemode & 61440) | ?),\
            acl = if(length(?) = 0, acl, ?),\
            ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?"; // 61440 is ~S_IFMT in decimal
const char* STMT_DELETE_REPLICA =
    "DELETE FROM Cns_file_replica\
        WHERE fileid = ? AND sfn = ?";
const char* STMT_DELETE_ALL_REPLICAS =
    "DELETE FROM Cns_file_replica\
        WHERE fileid = ?";
const char* STMT_ADD_REPLICA =
    "INSERT INTO Cns_file_replica\
          (fileid, nbaccesses,\
           ctime, atime, ptime, ltime,\
           r_type, status, f_type,\
           setname, poolname, host, fs, sfn, xattr)\
        VALUES\
          (?, 0,\
           UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(),\
           ?, ?, ?,\
           ?, ?, ?, ?, ?, ?)";
const char* STMT_TRUNCATE_FILE =
    "UPDATE Cns_file_metadata\
        SET filesize = 0, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_CHANGE_NAME =
    "UPDATE Cns_file_metadata\
        SET name = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_CHANGE_PARENT =
    "UPDATE Cns_file_metadata\
        SET parent_fileid = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_UTIME =
    "UPDATE Cns_file_metadata\
        SET atime = ?, mtime = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_UPDATE_REPLICA =
    "UPDATE Cns_file_replica\
        SET nbaccesses = ?, ctime = UNIX_TIMESTAMP(), atime = ?, ptime = ?, ltime = ?, \
            f_type = ?, status = ?, poolname = ?, \
            host = ?, fs = ?, sfn = ?, xattr = ?, setname = ?\
        WHERE rowid = ?";
const char* STMT_CHANGE_SIZE =
  "UPDATE Cns_file_metadata\
        SET filesize = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_INCREMENT_SIZE =
  "UPDATE Cns_file_metadata\
        SET filesize = filesize + ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
const char* STMT_CHANGE_CHECKSUM =
  "UPDATE Cns_file_metadata\
        SET ctime = UNIX_TIMESTAMP(), csumtype = ?, csumvalue = ?\
        WHERE fileid = ?";

// Authn

const char* STMT_GET_ALL_USERS =
    "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
        FROM Cns_userinfo";
const char* STMT_GET_USERINFO_BY_NAME =
    "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
        FROM Cns_userinfo\
        WHERE username = ?";
const char* STMT_GET_USERINFO_BY_UID =
    "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
        FROM Cns_userinfo\
        WHERE userid = ?";
const char* STMT_GET_ALL_GROUPS =
    "SELECT gid, groupname, banned, COALESCE(xattr, '')\
        FROM Cns_groupinfo";
const char* STMT_GET_GROUPINFO_BY_NAME =
    "SELECT gid, groupname, banned, COALESCE(xattr, '')\
        FROM Cns_groupinfo\
        WHERE groupname = ?";
const char* STMT_GET_GROUPINFO_BY_GID =
    "SELECT gid, groupname, banned, COALESCE(xattr, '')\
        FROM Cns_groupinfo\
        WHERE gid = ?";
const char* STMT_GET_UNIQ_UID_FOR_UPDATE =
    "SELECT id FROM Cns_unique_uid FOR UPDATE";
const char* STMT_GET_UNIQ_GID_FOR_UPDATE =
    "SELECT id FROM Cns_unique_gid FOR UPDATE";
const char* STMT_UPDATE_UNIQ_UID =
    "UPDATE Cns_unique_uid SET id = ?";
const char* STMT_UPDATE_UNIQ_GID =
    "UPDATE Cns_unique_gid SET id = ?";
const char* STMT_INSERT_UNIQ_UID =
    "INSERT INTO Cns_unique_uid (id) VALUES (?)";
const char* STMT_INSERT_UNIQ_GID =
    "INSERT INTO Cns_unique_gid (id) VALUES (?)";
const char* STMT_INSERT_USER =
    "INSERT INTO Cns_userinfo\
          (userid, username, user_ca, banned)\
        VALUES\
          (?, ?, '', ?)";
const char* STMT_UPDATE_USER =
    "UPDATE Cns_userinfo\
        SET banned = ?, xattr = ?\
        WHERE username = ?";
const char* STMT_DELETE_USER =
    "DELETE FROM Cns_userinfo\
        WHERE username = ?";
const char* STMT_INSERT_GROUP =
    "INSERT INTO Cns_groupinfo\
          (gid, groupname, banned)\
        VALUES\
          (?, ?, ?)";
const char* STMT_UPDATE_GROUP =
    "UPDATE Cns_groupinfo\
        SET banned = ?, xattr = ?\
        WHERE groupname = ?";
const char* STMT_DELETE_GROUP =
    "DELETE FROM Cns_groupinfo\
        WHERE groupname = ?";

// Pool queries

const char* STMT_GET_POOLS =
    "SELECT poolname, defsize, gc_start_thresh, gc_stop_thresh,\
        def_lifetime, defpintime, max_lifetime, maxpintime, fss_policy,\
        gc_policy, mig_policy, rs_policy, groups, ret_policy, s_type,\
        COALESCE(pooltype, 'filesystem'), COALESCE(poolmeta, '')\
        FROM dpm_pool";
const char* STMT_GET_POOL =
    "SELECT poolname, defsize, gc_start_thresh, gc_stop_thresh,\
        def_lifetime, defpintime, max_lifetime, maxpintime, fss_policy,\
        gc_policy, mig_policy, rs_policy, groups, ret_policy, s_type,\
        COALESCE(pooltype, 'filesystem'), COALESCE(poolmeta, '')\
        FROM dpm_pool where poolname = ?";
const char* STMT_INSERT_POOL =
    "INSERT INTO dpm_pool\
        (poolname, defsize, gc_start_thresh, gc_stop_thresh,\
        def_lifetime, defpintime, max_lifetime, maxpintime, fss_policy,\
        gc_policy, mig_policy, rs_policy, groups, ret_policy, s_type,\
        pooltype, poolmeta)\
      VALUES\
        (?, ?, ?, ?,\
         ?, ?, ?, ?, ?,\
         ?, ?, ?, ?, ?, ?,\
         ?, ?)";
const char* STMT_UPDATE_POOL =
    "UPDATE dpm_pool\
        SET defsize = ?, gc_start_thresh = ?, gc_stop_thresh = ?,\
            def_lifetime = ?, defpintime = ?, max_lifetime = ?,\
            maxpintime = ?, fss_policy = ?, gc_policy = ?,\
            mig_policy = ?, rs_policy = ?, groups = ?, ret_policy = ?,\
            s_type = ?, pooltype = ?, poolmeta = ?\
        WHERE poolname = ?";
const char* STMT_DELETE_POOL =
    "DELETE FROM dpm_pool\
        WHERE poolname = ?";
