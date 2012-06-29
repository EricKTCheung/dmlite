/// @file    plugins/mysql/Queries.cpp
/// @brief   MYSQL queries.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>


const char* STMT_GET_FILE_BY_ID =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE fileid = ?";
const char* STMT_GET_FILE_BY_GUID =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE guid = ?";
const char* STMT_GET_FILE_BY_NAME =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
            filesize, atime, mtime, ctime, fileclass, status,\
            csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ? AND name = ?";
const char* STMT_GET_LIST_FILES =
    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ?";
const char* STMT_GET_SYMLINK =
    "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = ?";
const char* STMT_GET_USERINFO_BY_NAME =
    "SELECT userid, username, user_ca, banned\
        FROM Cns_userinfo\
        WHERE username = ?";
const char* STMT_GET_USERINFO_BY_UID =
    "SELECT userid, username, user_ca, banned\
        FROM Cns_userinfo\
        WHERE userid = ?";
const char* STMT_GET_GROUPINFO_BY_NAME =
    "SELECT gid, groupname, banned\
        FROM Cns_groupinfo\
        WHERE groupname = ?";
const char* STMT_GET_GROUPINFO_BY_GID =
    "SELECT gid, groupname, banned\
        FROM Cns_groupinfo\
        WHERE gid = ?";
const char* STMT_GET_FILE_REPLICAS =
    "SELECT rowid, fileid, nbaccesses,\
            atime, ptime, ltime,\
            status, f_type, poolname, host, fs, sfn\
        FROM Cns_file_replica\
        WHERE fileid = ?";
const char* STMT_GET_REPLICA_BY_URL =
    "SELECT rowid, fileid, nbaccesses,\
            atime, ptime, ltime,\
            status, f_type, poolname, host, fs, sfn\
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
           csumtype, csumvalue, acl)\
        VALUES\
          (?, ?, ?, ?, ?, ?, ?,\
           ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), ?, ?,\
           ?, ?, ?)";
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
        SET owner_uid = ?, gid = ?, filemode = ?, acl = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
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
           setname, poolname, host, fs, sfn)\
        VALUES\
          (?, 0,\
           UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(),\
           ?, ?, ?,\
           ?, ?, ?, ?, ?)";
const char* STMT_TRUNCATE_FILE =
    "UPDATE Cns_file_metadata\
        SET filesize = 0, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";
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
          (?, ?, ?, ?)";
const char* STMT_INSERT_GROUP =
    "INSERT INTO Cns_groupinfo\
          (gid, groupname, banned)\
        VALUES\
          (?, ?, ?)";
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
        SET atime = ?, ltime = ?, nbaccesses = ?, status = ?, f_type = ?\
        WHERE rowid = ?";
const char* STMT_CHANGE_SIZE =
  "UPDATE Cns_file_metadata\
        SET filesize = ?, ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?";

// Pool queries
const char* STMT_GET_POOLS =
    "SELECT poolname, COALESCE(pooltype, 'filesystem')\
        FROM dpm_pool";
const char* STMT_GET_POOLS_LEGACY =
    "SELECT poolname, 'filesystem'\
        FROM dpm_pool";
const char* STMT_GET_POOL =
  "SELECT poolname, COALESCE(pooltype, 'filesystem')\
        FROM dpm_pool\
        where poolname = ?";
const char* STMT_GET_POOL_META=
  "SELECT poolmeta\
        FROM dpm_pool\
        WHERE poolname = ?";
