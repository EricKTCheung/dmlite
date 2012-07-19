/*
 * pydm_errno.cpp
 *
 * Python bindings for dm_errno.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	scope().attr("DM_NO_ERROR") = DM_NO_ERROR;

	scope().attr("DM_UNKNOWN_ERROR") = DM_UNKNOWN_ERROR;
	scope().attr("DM_UNEXPECTED_EXCEPTION") = DM_UNEXPECTED_EXCEPTION;
	scope().attr("DM_INTERNAL_ERROR") = DM_INTERNAL_ERROR;
	scope().attr("DM_NO_SUCH_SYMBOL") = DM_NO_SUCH_SYMBOL;
	scope().attr("DM_API_VERSION_MISMATCH") = DM_API_VERSION_MISMATCH;
	scope().attr("DM_NO_FACTORY") = DM_NO_FACTORY;
	scope().attr("DM_NO_POOL_MANAGER") = DM_NO_POOL_MANAGER;
	scope().attr("DM_NO_CATALOG") = DM_NO_CATALOG;
	scope().attr("DM_NO_INODE") = DM_NO_INODE;
	scope().attr("DM_NO_USERGROUPDB") = DM_NO_USERGROUPDB;
	scope().attr("DM_NO_SECURITY_CONTEXT") = DM_NO_SECURITY_CONTEXT;
	scope().attr("DM_NO_IO") = DM_NO_IO;

	scope().attr("DM_MALFORMED_CONF") = DM_MALFORMED_CONF;
	scope().attr("DM_UNKNOWN_OPTION") = DM_UNKNOWN_OPTION;
	scope().attr("DM_UNKNOWN_KEY") = DM_UNKNOWN_KEY;

	scope().attr("DM_UNKNOWN_HOST") = DM_UNKNOWN_HOST;
	scope().attr("DM_CONNECTION_ERROR") = DM_CONNECTION_ERROR;
	scope().attr("DM_SERVICE_UNAVAILABLE") = DM_SERVICE_UNAVAILABLE;
	scope().attr("DM_QUERY_FAILED") = DM_QUERY_FAILED;
	scope().attr("DM_UNKNOWN_FIELD") = DM_UNKNOWN_FIELD;

	scope().attr("DM_NOT_IMPLEMENTED") = DM_NOT_IMPLEMENTED;
	scope().attr("DM_NULL_POINTER") = DM_NULL_POINTER;
	scope().attr("DM_BAD_OPERATION") = DM_BAD_OPERATION;
	scope().attr("DM_NO_SUCH_FILE") = DM_NO_SUCH_FILE;
	scope().attr("DM_BAD_DESCRIPTOR") = DM_BAD_DESCRIPTOR;
	scope().attr("DM_RESOURCE_UNAVAILABLE") = DM_RESOURCE_UNAVAILABLE;
	scope().attr("DM_OUT_OF_MEMORY") = DM_OUT_OF_MEMORY;
	scope().attr("DM_FORBIDDEN") = DM_FORBIDDEN;
	scope().attr("DM_EXISTS") = DM_EXISTS;
	scope().attr("DM_NOT_DIRECTORY") = DM_NOT_DIRECTORY;
	scope().attr("DM_IS_DIRECTORY") = DM_IS_DIRECTORY;
	scope().attr("DM_INVALID_VALUE") = DM_INVALID_VALUE;
	scope().attr("DM_NO_SPACE_LEFT") = DM_NO_SPACE_LEFT;
	scope().attr("DM_NAME_TOO_LONG") = DM_NAME_TOO_LONG;
	scope().attr("DM_TOO_MANY_SYMLINKS") = DM_TOO_MANY_SYMLINKS;
	scope().attr("DM_NO_COMMENT") = DM_NO_COMMENT;
	scope().attr("DM_NO_REPLICAS") = DM_NO_REPLICAS;
	scope().attr("DM_PUT_ERROR") = DM_PUT_ERROR;
	scope().attr("DM_IS_CWD") = DM_IS_CWD;
	scope().attr("DM_NO_SUCH_REPLICA") = DM_NO_SUCH_REPLICA;
	
	scope().attr("DM_NO_USER_MAPPING") = DM_NO_USER_MAPPING;
	scope().attr("DM_NO_SUCH_USER") = DM_NO_SUCH_USER;
	scope().attr("DM_NO_SUCH_GROUP") = DM_NO_SUCH_GROUP;

	scope().attr("DM_INVALID_ACL") = DM_INVALID_ACL;

	scope().attr("DM_UNKNOWN_POOL_TYPE") = DM_UNKNOWN_POOL_TYPE;
	scope().attr("DM_NO_SUCH_FS") = DM_NO_SUCH_FS;
	scope().attr("DM_NO_SUCH_POOL") = DM_NO_SUCH_POOL;
