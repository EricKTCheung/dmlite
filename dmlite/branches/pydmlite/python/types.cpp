/*
 * types.cpp
 *
 * Python bindings for urls.h and security.h from the
 * c++ dmlite library via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<Url>("Url", init<const std::string&>())
		.def_readwrite("scheme", &Url::scheme)
		.def_readwrite("domain", &Url::domain)
		.def_readwrite("port", &Url::port)
		.def_readwrite("path", &Url::path)
		.def_readwrite("query", &Url::query)
		.def("splitPath", &Url::splitPath)
		.def("joinPath", &Url::joinPath)
		.def("normalizePath", &Url::normalizePath)
	;


    enum_<TokenResult>("TokenResult")
        .value("kTokenOK", kTokenOK)
        .value("kTokenMalformed", kTokenMalformed)
        .value("kTokenInvalid", kTokenInvalid)
        .value("kTokenExpired", kTokenExpired)
        .value("kTokenInvalidMode", kTokenInvalidMode)
        .value("kTokenInternalError", kTokenInternalError)
        ;

    enum_<AclEntry::AclType>("AclType")
        .value("kUserObj", AclEntry::kUserObj)
        .value("kUser", AclEntry::kUser)
        .value("kGroupObj", AclEntry::kGroupObj)
        .value("kGroup", AclEntry::kGroup)
        .value("kMask", AclEntry::kMask)
        .value("kOther", AclEntry::kOther)
        .value("kDefault", AclEntry::kDefault)
        ;

	class_<AclEntry>("AclEntry", init<>())
		.def_readwrite("type", &AclEntry::type)
		.def_readwrite("perm", &AclEntry::perm)
		.def_readwrite("id", &AclEntry::id)
        ;

	class_<Acl>("Acl", init< const std::string& >())
		.def(init< const Acl&, uid_t, gid_t, mode_t, mode_t* >())
		.def("has", &Acl::has)
		.def("serialize", &Acl::serialize)
		.def("validate", &Acl::validate)
		;

/*  // earlier version of this file
	scope().attr("HOST_NAME_MAX") = HOST_NAME_MAX;
	scope().attr("SCHEME_MAX") = SCHEME_MAX;
	scope().attr("TOKEN_MAX") = TOKEN_MAX;
	scope().attr("COMMENT_MAX") = COMMENT_MAX;
	scope().attr("GROUP_NAME_MAX") = GROUP_NAME_MAX;
	scope().attr("USER_NAME_MAX") = USER_NAME_MAX;
	scope().attr("URI_MAX") = URI_MAX;
	scope().attr("GUID_MAX") = GUID_MAX;
	scope().attr("ACL_ENTRIES_MAX") = ACL_ENTRIES_MAX;
	scope().attr("ACL_SIZE") = ACL_SIZE;
	scope().attr("POLICY_MAX") = POLICY_MAX;
	scope().attr("POOL_TYPE_MAX") = POOL_TYPE_MAX;
	scope().attr("POOL_MAX") = POOL_MAX;
	scope().attr("FILESYSTEM_MAX") = FILESYSTEM_MAX;
	scope().attr("SUMTYPE_MAX") = SUMTYPE_MAX;
	scope().attr("SUMVALUE_MAX") = SUMVALUE_MAX;
	scope().attr("SETNAME_MAX") = SETNAME_MAX;
	scope().attr("QUERY_MAX") = QUERY_MAX;
	scope().attr("TYPE_EXPERIMENT") = TYPE_EXPERIMENT;
	scope().attr("TYPE_USER") = TYPE_USER;
	scope().attr("STATUS_ONLINE") = STATUS_ONLINE;
	scope().attr("STATUS_MIGRATED") = STATUS_MIGRATED;

	class_<xstat>("ExtendedStat", init<>())
		.def_readwrite("stat", &xstat::stat)
		.def_readwrite("parent", &xstat::parent)
		.def_readwrite("type", &xstat::type)
		.def_readwrite("status", &xstat::status)
		.def_readonly("name", &xstat::name)
		.def_readonly("guid", &xstat::guid)
		.def_readonly("csumtype", &xstat::csumtype)
		.def_readonly("csumvalue", &xstat::csumvalue)
		.def_readonly("acl", &xstat::acl)
		;

	class_<SymLink>("SymLink", init<>())
		.def_readwrite("inode", &symlink::inode)
		.def_readonly("link", &symlink::link)
		;

	class_<filereplica>("FileReplica", init<>())
		.def_readwrite("replicaid", &filereplica::replicaid)
		.def_readwrite("fileid", &filereplica::fileid)
		.def_readwrite("nbaccesses", &filereplica::nbaccesses)
		.def_readwrite("atime", &filereplica::atime)
		.def_readwrite("ptime", &filereplica::ptime)
		.def_readwrite("ltime", &filereplica::ltime)
		.def_readwrite("status", &filereplica::status)
		.def_readwrite("type", &filereplica::type)
		.def_readonly("pool", &filereplica::pool)
		.def_readonly("server", &filereplica::server)
		.def_readonly("filesystem", &filereplica::filesystem)
		.def_readonly("rfn", &filereplica::rfn)
		.def_readwrite("priority", &filereplica::priority)
		;

	class_<keyvalue>("keyvalue", init<>())
		.def_readwrite("key", &keyvalue::key)
		.def_readwrite("value", &keyvalue::value)
		;

	class_<location>("location", init<>())
		.def_readonly("host", &location::host)
		.def_readonly("path", &location::path)
		.def_readwrite("nextra", &location::nextra)
		.def_readwrite("extra", &location::extra)
		.def_readwrite("available", &location::available)
		.def_readwrite("priority", &location::priority)
		;

	class_<url>("Url", init<>())
		.def_readonly("scheme", &url::scheme)
		.def_readonly("host", &url::host)
		.def_readwrite("port", &url::port)
		.def_readonly("path", &url::path)
		.def_readonly("query", &url::query)
		;

	class_<pool>("Pool", init<>())
		.def_readonly("pool_type", &pool::pool_type)
		.def_readonly("pool_name", &pool::pool_name)
		;

	class_<userinfo>("UserInfo", init<>())
		.def_readwrite("uid", &userinfo::uid)
		.def_readonly("name", &userinfo::name)
		.def_readonly("ca", &userinfo::ca)
		.def_readwrite("banned", &userinfo::banned)
		;

	class_<groupinfo>("GroupInfo", init<>())
		.def_readwrite("gid", &groupinfo::gid)
		.def_readonly("name", &groupinfo::name)
		.def_readwrite("banned", &groupinfo::banned)
		;

	scope().attr("CRED_MECH_NONE") = CRED_MECH_NONE;
	scope().attr("CRED_MECH_X509") = CRED_MECH_X509;

	class_<credentials>("Credentials", init<>())
		.def_readonly("mech", &credentials::mech)
		.def_readonly("client_name", &credentials::client_name)
		.def_readonly("remote_addr", &credentials::remote_addr)
		.def_readonly("fqans", &credentials::fqans)
		.def_readwrite("nfqans", &credentials::nfqans)
		.def_readonly("session_id", &credentials::session_id)
		.def_readonly("cred_data", &credentials::cred_data)
		;

	class_<dm_acl>("Acl", init<>())
		.def_readwrite("type", &dm_acl::type)
		.def_readwrite("perm", &dm_acl::perm)
		.def_readwrite("id", &dm_acl::id)
		;


	// bindings for union 'value' still have to be exposed
	
	class_<Location>("Location", init<>())
		.def(init<const struct location&>())
		.def(init<const Url&>())
		.def(init<const char*, const char*, bool, unsigned int>())

		.def_readonly("host", &Location::host)
		.def_readonly("path", &Location::path)
		.def_readwrite("nextra", &Location::nextra)
		.def_readwrite("extra", &Location::extra)
		.def_readwrite("available", &Location::available)
		.def_readwrite("priority", &Location::priority)
		;
		
	scope().attr("ACL_USER_OBJ") = ACL_USER_OBJ;
	scope().attr("ACL_USER") = ACL_USER;
	scope().attr("ACL_GROUP_OBJ") = ACL_GROUP_OBJ;
	scope().attr("ACL_GROUP") = ACL_GROUP;
	scope().attr("ACL_MASK") = ACL_MASK;
	scope().attr("ACL_OTHER") = ACL_OTHER;
	scope().attr("ACL_DEFAULT") = ACL_DEFAULT;
*/
