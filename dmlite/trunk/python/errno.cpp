/*
 * errno.cpp
 *
 * Python bindings for errno.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

    scope().attr("DMLITE_SUCCESS") = DMLITE_SUCCESS;
    
    scope().attr("DMLITE_USER_ERROR") = DMLITE_USER_ERROR;
    scope().attr("DMLITE_SYSTEM_ERROR") = DMLITE_SYSTEM_ERROR;
    scope().attr("DMLITE_CONFIGURATION_ERROR") = DMLITE_CONFIGURATION_ERROR;
    scope().attr("DMLITE_DATABASE_ERROR") = DMLITE_DATABASE_ERROR;

    scope().attr("DMLITE_UNKNOWN_ERROR") = DMLITE_UNKNOWN_ERROR;
    scope().attr("DMLITE_UNEXPECTED_EXCEPTION") = DMLITE_UNEXPECTED_EXCEPTION;
    scope().attr("DMLITE_INTERNAL_ERROR") = DMLITE_INTERNAL_ERROR;
    
    scope().attr("DMLITE_NO_SUCH_SYMBOL") = DMLITE_NO_SUCH_SYMBOL;
    scope().attr("DMLITE_API_VERSION_MISMATCH") = DMLITE_API_VERSION_MISMATCH;
    scope().attr("DMLITE_NO_POOL_MANAGER") = DMLITE_NO_POOL_MANAGER;
    scope().attr("DMLITE_NO_CATALOG") = DMLITE_NO_CATALOG;
    scope().attr("DMLITE_NO_INODE") = DMLITE_NO_INODE;
    scope().attr("DMLITE_NO_AUTHN") = DMLITE_NO_AUTHN;
    scope().attr("DMLITE_NO_IO") = DMLITE_NO_IO;
    
    scope().attr("DMLITE_NO_SECURITY_CONTEXT") = DMLITE_NO_SECURITY_CONTEXT;
    scope().attr("DMLITE_EMPTY_SECURITY_CONTEXT") = DMLITE_EMPTY_SECURITY_CONTEXT;
    
    scope().attr("DMLITE_MALFORMED") = DMLITE_MALFORMED;
    scope().attr("DMLITE_UNKNOWN_KEY") = DMLITE_UNKNOWN_KEY;

    scope().attr("DMLITE_NO_COMMENT") = DMLITE_NO_COMMENT;
    scope().attr("DMLITE_NO_REPLICAS") = DMLITE_NO_REPLICAS;
    scope().attr("DMLITE_NO_SUCH_REPLICA") = DMLITE_NO_SUCH_REPLICA;
    
    scope().attr("DMLITE_NO_USER_MAPPING") = DMLITE_NO_USER_MAPPING;
    scope().attr("DMLITE_NO_SUCH_USER") = DMLITE_NO_SUCH_USER;
    scope().attr("DMLITE_NO_SUCH_GROUP") = DMLITE_NO_SUCH_GROUP;
    scope().attr("DMLITE_INVALID_ACL") = DMLITE_INVALID_ACL;
    
    scope().attr("DMLITE_UNKNOWN_POOL_TYPE") = DMLITE_UNKNOWN_POOL_TYPE;
    scope().attr("DMLITE_NO_SUCH_POOL") = DMLITE_NO_SUCH_POOL;
