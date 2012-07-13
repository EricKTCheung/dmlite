/// @file    plugins/mysql/Queries.h
/// @brief   MYSQL queries.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef QUERIES_H
#define	QUERIES_H

extern const char* STMT_GET_FILE_BY_ID;
extern const char* STMT_GET_FILE_BY_GUID;
extern const char* STMT_GET_FILE_BY_NAME;
extern const char* STMT_GET_LIST_FILES;
extern const char* STMT_GET_SYMLINK;
extern const char* STMT_GET_USERINFO_BY_NAME;
extern const char* STMT_GET_USERINFO_BY_UID;
extern const char* STMT_GET_GROUPINFO_BY_NAME;
extern const char* STMT_GET_GROUPINFO_BY_GID;
extern const char* STMT_GET_FILE_REPLICAS;
extern const char* STMT_GET_REPLICA_BY_URL;
extern const char* STMT_GET_COMMENT;
extern const char* STMT_SET_GUID;
extern const char* STMT_SET_COMMENT;
extern const char* STMT_INSERT_COMMENT;
extern const char* STMT_INSERT_FILE;
extern const char* STMT_INSERT_SYMLINK;
extern const char* STMT_SELECT_UNIQ_ID_FOR_UPDATE;
extern const char* STMT_UPDATE_UNIQ_ID;
extern const char* STMT_INSERT_UNIQ_ID;
extern const char* STMT_DELETE_FILE;
extern const char* STMT_DELETE_COMMENT;
extern const char* STMT_DELETE_SYMLINK;
extern const char* STMT_NLINK_FOR_UPDATE;
extern const char* STMT_UPDATE_NLINK;
extern const char* STMT_UPDATE_PERMS;
extern const char* STMT_DELETE_REPLICA;
extern const char* STMT_DELETE_ALL_REPLICAS;
extern const char* STMT_ADD_REPLICA;
extern const char* STMT_TRUNCATE_FILE;
extern const char* STMT_GET_UNIQ_UID_FOR_UPDATE;
extern const char* STMT_GET_UNIQ_GID_FOR_UPDATE;
extern const char* STMT_UPDATE_UNIQ_UID;
extern const char* STMT_UPDATE_UNIQ_GID;
extern const char* STMT_INSERT_UNIQ_UID;
extern const char* STMT_INSERT_UNIQ_GID;
extern const char* STMT_INSERT_USER;;
extern const char* STMT_INSERT_GROUP;
extern const char* STMT_CHANGE_NAME;
extern const char* STMT_CHANGE_PARENT;
extern const char* STMT_UTIME;
extern const char* STMT_UPDATE_REPLICA;
extern const char* STMT_CHANGE_SIZE;
extern const char* STMT_CHANGE_CHECKSUM;

// Pool queries
extern const char* STMT_GET_POOLS;
extern const char* STMT_GET_POOLS_LEGACY;
extern const char* STMT_GET_POOL;
extern const char* STMT_GET_POOL_META;

#endif	// QUERIES_H

