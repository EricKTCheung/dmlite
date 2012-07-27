/*
 * pydmlite.cpp
 *
 * Python bindings for the c++ dmlite library via Boost:Python.
 * The resulting python module pydmlite gives access to most
 * the available functionality from dmlite.
 */

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

#include "authn.h"
#include "base.h"
#include "catalog.h"
#include "dmlite.h"
#include "exceptions.h"
#include "inode.h"
#include "io.h"
#include "pooldriver.h"
#include "poolmanager.h"

using namespace dmlite;
using namespace boost::python;


// include wrapper classes
#include "helpers.cpp"
#include "basewrapper.cpp"
#include "catalogwrapper.cpp"


BOOST_PYTHON_MODULE(pydmlite)
{

	scope().attr("API_VERSION") = API_VERSION; 
	
	class_<PluginManager, boost::noncopyable>("PluginManager")
		.def("loadPlugin", &PluginManager::loadPlugin)
		.def("configure", &PluginManager::configure)
		.def("loadConfiguration", &PluginManager::loadConfiguration)

		.def("registerAuthnFactory", &PluginManager::registerAuthnFactory)
		.def("registerINodeFactory", &PluginManager::registerINodeFactory)
		.def("registerCatalogFactory", &PluginManager::registerCatalogFactory)
		.def("registerPoolManagerFactory", &PluginManager::registerPoolManagerFactory)
		.def("registerIOFactory", &PluginManager::registerIOFactory)
		.def("registerPoolDriverFactory", &PluginManager::registerPoolDriverFactory)
		
		.def("getAuthnFactory", &PluginManager::getAuthnFactory, return_value_policy<reference_existing_object>())
		.def("getINodeFactory", &PluginManager::getINodeFactory, return_value_policy<reference_existing_object>())
		.def("getCatalogFactory", &PluginManager::getCatalogFactory, return_value_policy<reference_existing_object>())
		.def("getPoolManagerFactory", &PluginManager::getPoolManagerFactory, return_value_policy<reference_existing_object>())
		.def("getPoolDriverFactory", &PluginManager::getPoolDriverFactory, return_value_policy<reference_existing_object>())
		.def("getIOFactory", &PluginManager::getIOFactory, return_value_policy<reference_existing_object>())
		;
	
	class_<StackInstance>("StackInstance", init<PluginManager*>())
		.def("set", &StackInstance::set)
		.def("get", &StackInstance::get)
		.def("erase", &StackInstance::erase)
		.def("getPluginManager", &StackInstance::getPluginManager, return_value_policy<reference_existing_object>())
		.def("setSecurityCredentials", &StackInstance::setSecurityCredentials)
		.def("setSecurityContext", &StackInstance::setSecurityContext)
		.def("getSecurityContext", &StackInstance::getSecurityContext, return_value_policy<reference_existing_object>())
		.def("getAuthn", &StackInstance::getAuthn, return_value_policy<reference_existing_object>())
		.def("getINode", &StackInstance::getINode, return_value_policy<reference_existing_object>())
		.def("getCatalog", &StackInstance::getCatalog, return_value_policy<reference_existing_object>())
		.def("isTherePoolManager", &StackInstance::isTherePoolManager)
		.def("getPoolManager", &StackInstance::getPoolManager, return_value_policy<reference_existing_object>())
		.def("getPoolDriver", &StackInstance::getPoolDriver, return_value_policy<reference_existing_object>())
		.def("getIODriver", &StackInstance::getIODriver, return_value_policy<reference_existing_object>())
		;

	class_<PluginIdCard>("PluginIdCard")
		.def_readonly("ApiVersion", &PluginIdCard::ApiVersion)
		// registerPlugin not exposed yet
		;

	scope().attr("PLUGIN_ID_HEADER") = PLUGIN_ID_HEADER; 


#include "extensible.cpp"

#include "authn.cpp"
#include "base.cpp"
#include "catalog.cpp"

/*
#include "exceptions.cpp"
#include "inode.cpp"
#include "io.cpp"
#include "pool.cpp"
#include "pooldriver.cpp"

#include "errno.cpp"
#include "types.cpp"
*/

}














/*

The following code was auto generated via pyplusplus. Compilation gives hundreds of errors and it is quite confusing, which makes it hard to fix, but it can be used as a suggestion for the implementation.

#include "boost/python.hpp"
#include "boost/python/suite/indexing/vector_indexing_suite.hpp"
#include "boost/python/suite/indexing/map_indexing_suite.hpp"
#include "dmlite.h"

namespace bp = boost::python;

struct BaseFactory_wrapper : dmlite::BaseFactory, bp::wrapper< dmlite::BaseFactory > {

    BaseFactory_wrapper()
    : dmlite::BaseFactory()
      , bp::wrapper< dmlite::BaseFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

};

struct BaseInterface_wrapper : dmlite::BaseInterface, bp::wrapper< dmlite::BaseInterface > {

    BaseInterface_wrapper()
    : dmlite::BaseInterface()
      , bp::wrapper< dmlite::BaseInterface >(){
        // null constructor
        
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct Catalog_wrapper : dmlite::Catalog, bp::wrapper< dmlite::Catalog > {

    Catalog_wrapper()
    : dmlite::Catalog()
      , bp::wrapper< dmlite::Catalog >(){
        // null constructor
        
    }

    virtual void addReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_addReplica = this->get_override( "addReplica" );
        func_addReplica( boost::ref(replica) );
    }

    virtual void changeDir( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_changeDir = this->get_override( "changeDir" );
        func_changeDir( path );
    }

    virtual void closeDir( ::dmlite::Directory * dir ) throw( dmlite::DmException ){
        bp::override func_closeDir = this->get_override( "closeDir" );
        func_closeDir( dir );
    }

    virtual void create( ::std::string const & path, ::mode_t mode ) throw( dmlite::DmException ){
        bp::override func_create = this->get_override( "create" );
        func_create( path, mode );
    }

    virtual void deleteReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_deleteReplica = this->get_override( "deleteReplica" );
        func_deleteReplica( boost::ref(replica) );
    }

    virtual ::ExtendedStat extendedStat( ::std::string const & path, bool followSym=true ) throw( dmlite::DmException ){
        bp::override func_extendedStat = this->get_override( "extendedStat" );
        return func_extendedStat( path, followSym );
    }

    virtual ::std::string getComment( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_getComment = this->get_override( "getComment" );
        return func_getComment( path );
    }

    virtual ::FileReplica getReplica( ::std::string const & rfn ) throw( dmlite::DmException ){
        bp::override func_getReplica = this->get_override( "getReplica" );
        return func_getReplica( rfn );
    }

    virtual ::std::vector< filereplica > getReplicas( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_getReplicas = this->get_override( "getReplicas" );
        return func_getReplicas( path );
    }

    virtual ::std::string getWorkingDir(  ) throw( dmlite::DmException ){
        bp::override func_getWorkingDir = this->get_override( "getWorkingDir" );
        return func_getWorkingDir(  );
    }

    virtual void makeDir( ::std::string const & path, ::mode_t mode ) throw( dmlite::DmException ){
        bp::override func_makeDir = this->get_override( "makeDir" );
        func_makeDir( path, mode );
    }

    virtual ::dmlite::Directory * openDir( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_openDir = this->get_override( "openDir" );
        return func_openDir( path );
    }

    virtual ::dirent * readDir( ::dmlite::Directory * dir ) throw( dmlite::DmException ){
        bp::override func_readDir = this->get_override( "readDir" );
        return func_readDir( dir );
    }

    virtual ::ExtendedStat * readDirx( ::dmlite::Directory * dir ) throw( dmlite::DmException ){
        bp::override func_readDirx = this->get_override( "readDirx" );
        return func_readDirx( dir );
    }

    virtual void removeDir( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_removeDir = this->get_override( "removeDir" );
        func_removeDir( path );
    }

    virtual void rename( ::std::string const & oldPath, ::std::string const & newPath ) throw( dmlite::DmException ){
        bp::override func_rename = this->get_override( "rename" );
        func_rename( oldPath, newPath );
    }

    virtual void setAcl( ::std::string const & path, ::std::vector< dm_acl > const & acls ) throw( dmlite::DmException ){
        bp::override func_setAcl = this->get_override( "setAcl" );
        func_setAcl( path, boost::ref(acls) );
    }

    virtual void setChecksum( ::std::string const & path, ::std::string const & csumtype, ::std::string const & csumvalue ) throw( dmlite::DmException ){
        bp::override func_setChecksum = this->get_override( "setChecksum" );
        func_setChecksum( path, csumtype, csumvalue );
    }

    virtual void setComment( ::std::string const & path, ::std::string const & comment ) throw( dmlite::DmException ){
        bp::override func_setComment = this->get_override( "setComment" );
        func_setComment( path, comment );
    }

    virtual void setGuid( ::std::string const & path, ::std::string const & guid ) throw( dmlite::DmException ){
        bp::override func_setGuid = this->get_override( "setGuid" );
        func_setGuid( path, guid );
    }

    virtual void setMode( ::std::string const & path, ::mode_t mode ) throw( dmlite::DmException ){
        bp::override func_setMode = this->get_override( "setMode" );
        func_setMode( path, mode );
    }

    virtual void setOwner( ::std::string const & path, ::uid_t newUid, ::gid_t newGid, bool followSymLink=true ) throw( dmlite::DmException ){
        bp::override func_setOwner = this->get_override( "setOwner" );
        func_setOwner( path, newUid, newGid, followSymLink );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    virtual void setSize( ::std::string const & path, ::size_t newSize ) throw( dmlite::DmException ){
        bp::override func_setSize = this->get_override( "setSize" );
        func_setSize( path, newSize );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    virtual void symlink( ::std::string const & oldpath, ::std::string const & newpath ) throw( dmlite::DmException ){
        bp::override func_symlink = this->get_override( "symlink" );
        func_symlink( oldpath, newpath );
    }

    virtual ::mode_t umask( ::mode_t mask ) throw(){
        bp::override func_umask = this->get_override( "umask" );
        return func_umask( mask );
    }

    virtual void unlink( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_unlink = this->get_override( "unlink" );
        func_unlink( path );
    }

    virtual void updateReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_updateReplica = this->get_override( "updateReplica" );
        func_updateReplica( boost::ref(replica) );
    }

    virtual void utime( ::std::string const & path, ::utimbuf const * buf ) throw( dmlite::DmException ){
        bp::override func_utime = this->get_override( "utime" );
        func_utime( path, boost::python::ptr(buf) );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct CatalogFactory_wrapper : dmlite::CatalogFactory, bp::wrapper< dmlite::CatalogFactory > {

    CatalogFactory_wrapper()
    : dmlite::CatalogFactory()
      , bp::wrapper< dmlite::CatalogFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    static ::dmlite::Catalog * createCatalog( ::dmlite::CatalogFactory * factory, ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        return dmlite::CatalogFactory::createCatalog( boost::python::ptr(factory), boost::python::ptr(pm) );
    }

    virtual ::dmlite::Catalog * createCatalog( ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        bp::override func_createCatalog = this->get_override( "createCatalog" );
        return func_createCatalog( boost::python::ptr(pm) );
    }

};

struct DmException_wrapper : dmlite::DmException, bp::wrapper< dmlite::DmException > {

    DmException_wrapper( )
    : dmlite::DmException( )
      , bp::wrapper< dmlite::DmException >(){
        // null constructor
    
    }

    DmException_wrapper(int code )
    : dmlite::DmException( code )
      , bp::wrapper< dmlite::DmException >(){
        // constructor
    
    }

    DmException_wrapper(int code, ::std::string const & string )
    : dmlite::DmException( code, string )
      , bp::wrapper< dmlite::DmException >(){
        // constructor
    
    }

    DmException_wrapper(int code, char const * fmt, ::__va_list_tag * args )
    : dmlite::DmException( code, fmt, boost::python::ptr(args) )
      , bp::wrapper< dmlite::DmException >(){
        // constructor
    
    }

    DmException_wrapper(::dmlite::DmException const & de )
    : dmlite::DmException( boost::ref(de) )
      , bp::wrapper< dmlite::DmException >(){
        // copy constructor
    
    }

    void setMessage( char const * fmt, ::__va_list_tag * args ){
        dmlite::DmException::setMessage( fmt, boost::python::ptr(args) );
    }

    virtual char const * what(  ) const   throw(){
        if( bp::override func_what = this->get_override( "what" ) )
            return func_what(  );
        else
            return this->dmlite::DmException::what(  );
    }
    
    
    char const * default_what(  ) const   throw(){
        return dmlite::DmException::what( );
    }

};

struct INode_wrapper : dmlite::INode, bp::wrapper< dmlite::INode > {

    INode_wrapper()
    : dmlite::INode()
      , bp::wrapper< dmlite::INode >(){
        // null constructor
        
    }

    virtual void addReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_addReplica = this->get_override( "addReplica" );
        func_addReplica( boost::ref(replica) );
    }

    virtual void begin(  ) throw( dmlite::DmException ){
        bp::override func_begin = this->get_override( "begin" );
        func_begin(  );
    }

    virtual void closeDir( ::dmlite::IDirectory * dir ) throw( dmlite::DmException ){
        bp::override func_closeDir = this->get_override( "closeDir" );
        func_closeDir( dir );
    }

    virtual void commit(  ) throw( dmlite::DmException ){
        bp::override func_commit = this->get_override( "commit" );
        func_commit(  );
    }

    virtual ::ExtendedStat create( ::ino_t parent, ::std::string const & name, ::uid_t uid, ::gid_t gid, ::mode_t mode, ::size_t size, short int type, char status, ::std::string const & csumtype, ::std::string const & csumvalue, ::std::string const & acl ) throw( dmlite::DmException ){
        bp::override func_create = this->get_override( "create" );
        return func_create( parent, name, uid, gid, mode, size, type, status, csumtype, csumvalue, acl );
    }

    virtual void deleteComment( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_deleteComment = this->get_override( "deleteComment" );
        func_deleteComment( inode );
    }

    virtual void deleteReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_deleteReplica = this->get_override( "deleteReplica" );
        func_deleteReplica( boost::ref(replica) );
    }

    virtual ::ExtendedStat extendedStat( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_extendedStat = this->get_override( "extendedStat" );
        return func_extendedStat( inode );
    }

    virtual ::ExtendedStat extendedStat( ::ino_t parent, ::std::string const & name ) throw( dmlite::DmException ){
        bp::override func_extendedStat = this->get_override( "extendedStat" );
        return func_extendedStat( parent, name );
    }

    virtual ::ExtendedStat extendedStat( ::std::string const & guid ) throw( dmlite::DmException ){
        bp::override func_extendedStat = this->get_override( "extendedStat" );
        return func_extendedStat( guid );
    }

    virtual ::std::string getComment( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_getComment = this->get_override( "getComment" );
        return func_getComment( inode );
    }

    virtual ::FileReplica getReplica( ::int64_t rid ) throw( dmlite::DmException ){
        bp::override func_getReplica = this->get_override( "getReplica" );
        return func_getReplica( rid );
    }

    virtual ::FileReplica getReplica( ::std::string const & rfn ) throw( dmlite::DmException ){
        bp::override func_getReplica = this->get_override( "getReplica" );
        return func_getReplica( rfn );
    }

    virtual ::std::vector< filereplica > getReplicas( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_getReplicas = this->get_override( "getReplicas" );
        return func_getReplicas( inode );
    }

    virtual void move( ::ino_t inode, ::ino_t dest ) throw( dmlite::DmException ){
        bp::override func_move = this->get_override( "move" );
        func_move( inode, dest );
    }

    virtual ::dmlite::IDirectory * openDir( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_openDir = this->get_override( "openDir" );
        return func_openDir( inode );
    }

    virtual ::dirent * readDir( ::dmlite::IDirectory * dir ) throw( dmlite::DmException ){
        bp::override func_readDir = this->get_override( "readDir" );
        return func_readDir( dir );
    }

    virtual ::ExtendedStat * readDirx( ::dmlite::IDirectory * dir ) throw( dmlite::DmException ){
        bp::override func_readDirx = this->get_override( "readDirx" );
        return func_readDirx( dir );
    }

    virtual ::SymLink readLink( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_readLink = this->get_override( "readLink" );
        return func_readLink( inode );
    }

    virtual void rename( ::ino_t inode, ::std::string const & name ) throw( dmlite::DmException ){
        bp::override func_rename = this->get_override( "rename" );
        func_rename( inode, name );
    }

    virtual void rollback(  ) throw( dmlite::DmException ){
        bp::override func_rollback = this->get_override( "rollback" );
        func_rollback(  );
    }

    virtual void setChecksum( ::ino_t inode, ::std::string const & csumtype, ::std::string const & csumvalue ) throw( dmlite::DmException ){
        bp::override func_setChecksum = this->get_override( "setChecksum" );
        func_setChecksum( inode, csumtype, csumvalue );
    }

    virtual void setComment( ::ino_t inode, ::std::string const & comment ) throw( dmlite::DmException ){
        bp::override func_setComment = this->get_override( "setComment" );
        func_setComment( inode, comment );
    }

    virtual void setGuid( ::ino_t inode, ::std::string const & guid ) throw( dmlite::DmException ){
        bp::override func_setGuid = this->get_override( "setGuid" );
        func_setGuid( inode, guid );
    }

    virtual void setMode( ::ino_t inode, ::uid_t uid, ::gid_t gid, ::mode_t mode, ::std::string const & acl ) throw( dmlite::DmException ){
        bp::override func_setMode = this->get_override( "setMode" );
        func_setMode( inode, uid, gid, mode, acl );
    }

    virtual void setSize( ::ino_t inode, ::size_t size ) throw( dmlite::DmException ){
        bp::override func_setSize = this->get_override( "setSize" );
        func_setSize( inode, size );
    }

    virtual void symlink( ::ino_t inode, ::std::string const & link ) throw( dmlite::DmException ){
        bp::override func_symlink = this->get_override( "symlink" );
        func_symlink( inode, link );
    }

    virtual void unlink( ::ino_t inode ) throw( dmlite::DmException ){
        bp::override func_unlink = this->get_override( "unlink" );
        func_unlink( inode );
    }

    virtual void updateReplica( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_updateReplica = this->get_override( "updateReplica" );
        func_updateReplica( boost::ref(replica) );
    }

    virtual void utime( ::ino_t inode, ::utimbuf const * buf ) throw( dmlite::DmException ){
        bp::override func_utime = this->get_override( "utime" );
        func_utime( inode, boost::python::ptr(buf) );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct INodeFactory_wrapper : dmlite::INodeFactory, bp::wrapper< dmlite::INodeFactory > {

    INodeFactory_wrapper()
    : dmlite::INodeFactory()
      , bp::wrapper< dmlite::INodeFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    static ::dmlite::INode * createINode( ::dmlite::INodeFactory * factory, ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        return dmlite::INodeFactory::createINode( boost::python::ptr(factory), boost::python::ptr(pm) );
    }

    virtual ::dmlite::INode * createINode( ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        bp::override func_createINode = this->get_override( "createINode" );
        return func_createINode( boost::python::ptr(pm) );
    }

};

struct IODriver_wrapper : dmlite::IODriver, bp::wrapper< dmlite::IODriver > {

    IODriver_wrapper()
    : dmlite::IODriver()
      , bp::wrapper< dmlite::IODriver >(){
        // null constructor
        
    }

    virtual ::dmlite::IOHandler * createIOHandler( ::std::string const & pfn, int flags, ::std::map< std::string, std::string > const & extras ) throw( dmlite::DmException ){
        bp::override func_createIOHandler = this->get_override( "createIOHandler" );
        return func_createIOHandler( pfn, flags, boost::ref(extras) );
    }

    virtual ::stat pStat( ::std::string const & pfn ) throw( dmlite::DmException ){
        bp::override func_pStat = this->get_override( "pStat" );
        return func_pStat( pfn );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct IOFactory_wrapper : dmlite::IOFactory, bp::wrapper< dmlite::IOFactory > {

    IOFactory_wrapper()
    : dmlite::IOFactory()
      , bp::wrapper< dmlite::IOFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    virtual ::dmlite::IODriver * createIODriver( ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        bp::override func_createIODriver = this->get_override( "createIODriver" );
        return func_createIODriver( boost::python::ptr(pm) );
    }

};

struct IOHandler_wrapper : dmlite::IOHandler, bp::wrapper< dmlite::IOHandler > {

    IOHandler_wrapper()
    : dmlite::IOHandler()
      , bp::wrapper< dmlite::IOHandler >(){
        // null constructor
        
    }

    virtual void close(  ) throw( dmlite::DmException ){
        bp::override func_close = this->get_override( "close" );
        func_close(  );
    }

    virtual bool eof(  ) throw( dmlite::DmException ){
        bp::override func_eof = this->get_override( "eof" );
        return func_eof(  );
    }

    virtual void flush(  ) throw( dmlite::DmException ){
        bp::override func_flush = this->get_override( "flush" );
        func_flush(  );
    }

    virtual ::size_t read( char * buffer, ::size_t count ) throw( dmlite::DmException ){
        bp::override func_read = this->get_override( "read" );
        return func_read( buffer, count );
    }

    virtual void seek( long int offset, int whence ) throw( dmlite::DmException ){
        bp::override func_seek = this->get_override( "seek" );
        func_seek( offset, whence );
    }

    virtual long int tell(  ) throw( dmlite::DmException ){
        bp::override func_tell = this->get_override( "tell" );
        return func_tell(  );
    }

    virtual ::size_t write( char const * buffer, ::size_t count ) throw( dmlite::DmException ){
        bp::override func_write = this->get_override( "write" );
        return func_write( buffer, count );
    }

};

struct PoolDriver_wrapper : dmlite::PoolDriver, bp::wrapper< dmlite::PoolDriver > {

    PoolDriver_wrapper()
    : dmlite::PoolDriver()
      , bp::wrapper< dmlite::PoolDriver >(){
        // null constructor
        
    }

    virtual ::dmlite::PoolHandler * createPoolHandler( ::std::string const & poolName ) throw( dmlite::DmException ){
        bp::override func_createPoolHandler = this->get_override( "createPoolHandler" );
        return func_createPoolHandler( poolName );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct PoolDriverFactory_wrapper : dmlite::PoolDriverFactory, bp::wrapper< dmlite::PoolDriverFactory > {

    PoolDriverFactory_wrapper()
    : dmlite::PoolDriverFactory()
      , bp::wrapper< dmlite::PoolDriverFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    virtual ::dmlite::PoolDriver * createPoolDriver(  ) throw( dmlite::DmException ){
        bp::override func_createPoolDriver = this->get_override( "createPoolDriver" );
        return func_createPoolDriver(  );
    }

    virtual ::std::string implementedPool(  ) throw(){
        bp::override func_implementedPool = this->get_override( "implementedPool" );
        return func_implementedPool(  );
    }

};

struct PoolHandler_wrapper : dmlite::PoolHandler, bp::wrapper< dmlite::PoolHandler > {

    PoolHandler_wrapper()
    : dmlite::PoolHandler()
      , bp::wrapper< dmlite::PoolHandler >(){
        // null constructor
        
    }

    virtual ::uint64_t getFreeSpace(  ) throw( dmlite::DmException ){
        bp::override func_getFreeSpace = this->get_override( "getFreeSpace" );
        return func_getFreeSpace(  );
    }

    virtual ::Location getLocation( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_getLocation = this->get_override( "getLocation" );
        return func_getLocation( boost::ref(replica) );
    }

    virtual ::std::string getPoolName(  ) throw( dmlite::DmException ){
        bp::override func_getPoolName = this->get_override( "getPoolName" );
        return func_getPoolName(  );
    }

    virtual ::std::string getPoolType(  ) throw( dmlite::DmException ){
        bp::override func_getPoolType = this->get_override( "getPoolType" );
        return func_getPoolType(  );
    }

    virtual ::uint64_t getTotalSpace(  ) throw( dmlite::DmException ){
        bp::override func_getTotalSpace = this->get_override( "getTotalSpace" );
        return func_getTotalSpace(  );
    }

    virtual bool isAvailable( bool write=true ) throw( dmlite::DmException ){
        bp::override func_isAvailable = this->get_override( "isAvailable" );
        return func_isAvailable( write );
    }

    virtual void putDone( ::FileReplica const & replica, ::std::map< std::string, std::string > const & extras ) throw( dmlite::DmException ){
        bp::override func_putDone = this->get_override( "putDone" );
        func_putDone( boost::ref(replica), boost::ref(extras) );
    }

    virtual ::Location putLocation( ::std::string const & fn ) throw( dmlite::DmException ){
        bp::override func_putLocation = this->get_override( "putLocation" );
        return func_putLocation( fn );
    }

    virtual void remove( ::FileReplica const & replica ) throw( dmlite::DmException ){
        bp::override func_remove = this->get_override( "remove" );
        func_remove( boost::ref(replica) );
    }

};

struct PoolManager_wrapper : dmlite::PoolManager, bp::wrapper< dmlite::PoolManager > {

    PoolManager_wrapper()
    : dmlite::PoolManager()
      , bp::wrapper< dmlite::PoolManager >(){
        // null constructor
        
    }

    virtual void doneWriting( ::std::string const & host, ::std::string const & rfn, ::std::map< std::string, std::string > const & params ) throw( dmlite::DmException ){
        bp::override func_doneWriting = this->get_override( "doneWriting" );
        func_doneWriting( host, rfn, boost::ref(params) );
    }

    virtual ::Pool getPool( ::std::string const & poolname ) throw( dmlite::DmException ){
        bp::override func_getPool = this->get_override( "getPool" );
        return func_getPool( poolname );
    }

    virtual ::dmlite::PoolMetadata * getPoolMetadata( ::std::string const & poolName ) throw( dmlite::DmException ){
        bp::override func_getPoolMetadata = this->get_override( "getPoolMetadata" );
        return func_getPoolMetadata( poolName );
    }

    virtual ::std::vector< pool > getPools( ::dmlite::PoolManager::PoolAvailability availability=::dmlite::PoolManager::kAny ) throw( dmlite::DmException ){
        bp::override func_getPools = this->get_override( "getPools" );
        return func_getPools( availability );
    }

    virtual ::Location whereToRead( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_whereToRead = this->get_override( "whereToRead" );
        return func_whereToRead( path );
    }

    virtual ::Location whereToWrite( ::std::string const & path ) throw( dmlite::DmException ){
        bp::override func_whereToWrite = this->get_override( "whereToWrite" );
        return func_whereToWrite( path );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual void setSecurityContext( ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        bp::override func_setSecurityContext = this->get_override( "setSecurityContext" );
        func_setSecurityContext( boost::python::ptr(ctx) );
    }

    static void setSecurityContext( ::dmlite::BaseInterface * i, ::dmlite::SecurityContext const * ctx ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setSecurityContext( boost::python::ptr(i), boost::python::ptr(ctx) );
    }

    virtual void setStackInstance( ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        bp::override func_setStackInstance = this->get_override( "setStackInstance" );
        func_setStackInstance( boost::python::ptr(si) );
    }

    static void setStackInstance( ::dmlite::BaseInterface * i, ::dmlite::StackInstance * si ) throw( dmlite::DmException ){
        dmlite::BaseInterface::setStackInstance( boost::python::ptr(i), boost::python::ptr(si) );
    }

};

struct PoolManagerFactory_wrapper : dmlite::PoolManagerFactory, bp::wrapper< dmlite::PoolManagerFactory > {

    PoolManagerFactory_wrapper()
    : dmlite::PoolManagerFactory()
      , bp::wrapper< dmlite::PoolManagerFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    static ::dmlite::PoolManager * createPoolManager( ::dmlite::PoolManagerFactory * factory, ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        return dmlite::PoolManagerFactory::createPoolManager( boost::python::ptr(factory), boost::python::ptr(pm) );
    }

    virtual ::dmlite::PoolManager * createPoolManager( ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        bp::override func_createPoolManager = this->get_override( "createPoolManager" );
        return func_createPoolManager( boost::python::ptr(pm) );
    }

};

struct PoolMetadata_wrapper : dmlite::PoolMetadata, bp::wrapper< dmlite::PoolMetadata > {

    PoolMetadata_wrapper()
    : dmlite::PoolMetadata()
      , bp::wrapper< dmlite::PoolMetadata >(){
        // null constructor
        
    }

    virtual int getInt( ::std::string const & field ) throw( dmlite::DmException ){
        bp::override func_getInt = this->get_override( "getInt" );
        return func_getInt( field );
    }

    virtual ::std::string getString( ::std::string const & field ) throw( dmlite::DmException ){
        bp::override func_getString = this->get_override( "getString" );
        return func_getString( field );
    }

};

struct UserGroupDb_wrapper : dmlite::UserGroupDb, bp::wrapper< dmlite::UserGroupDb > {

    UserGroupDb_wrapper()
    : dmlite::UserGroupDb()
      , bp::wrapper< dmlite::UserGroupDb >(){
        // null constructor
        
    }

    virtual ::dmlite::SecurityContext * createSecurityContext( ::dmlite::SecurityCredentials const & cred ) throw( dmlite::DmException ){
        bp::override func_createSecurityContext = this->get_override( "createSecurityContext" );
        return func_createSecurityContext( boost::ref(cred) );
    }

    virtual ::GroupInfo getGroup( ::std::string const & groupName ) throw( dmlite::DmException ){
        bp::override func_getGroup = this->get_override( "getGroup" );
        return func_getGroup( groupName );
    }

    virtual void getIdMap( ::std::string const & userName, ::std::vector< std::string > const & groupNames, ::UserInfo * user, ::std::vector< groupinfo > * groups ) throw( dmlite::DmException ){
        bp::override func_getIdMap = this->get_override( "getIdMap" );
        func_getIdMap( userName, boost::ref(groupNames), boost::python::ptr(user), boost::python::ptr(groups) );
    }

    virtual ::std::string getImplId(  ) throw(){
        bp::override func_getImplId = this->get_override( "getImplId" );
        return func_getImplId(  );
    }

    virtual ::UserInfo getUser( ::std::string const & userName ) throw( dmlite::DmException ){
        bp::override func_getUser = this->get_override( "getUser" );
        return func_getUser( userName );
    }

    virtual ::GroupInfo newGroup( ::std::string const & gname ) throw( dmlite::DmException ){
        bp::override func_newGroup = this->get_override( "newGroup" );
        return func_newGroup( gname );
    }

    virtual ::UserInfo newUser( ::std::string const & uname, ::std::string const & ca ) throw( dmlite::DmException ){
        bp::override func_newUser = this->get_override( "newUser" );
        return func_newUser( uname, ca );
    }

};

struct UserGroupDbFactory_wrapper : dmlite::UserGroupDbFactory, bp::wrapper< dmlite::UserGroupDbFactory > {

    UserGroupDbFactory_wrapper()
    : dmlite::UserGroupDbFactory()
      , bp::wrapper< dmlite::UserGroupDbFactory >(){
        // null constructor
        
    }

    virtual void configure( ::std::string const & key, ::std::string const & value ) throw( dmlite::DmException ){
        bp::override func_configure = this->get_override( "configure" );
        func_configure( key, value );
    }

    static ::dmlite::UserGroupDb * createUserGroupDb( ::dmlite::UserGroupDbFactory * factory, ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        return dmlite::UserGroupDbFactory::createUserGroupDb( boost::python::ptr(factory), boost::python::ptr(pm) );
    }

    virtual ::dmlite::UserGroupDb * createUserGroupDb( ::dmlite::PluginManager * pm ) throw( dmlite::DmException ){
        bp::override func_createUserGroupDb = this->get_override( "createUserGroupDb" );
        return func_createUserGroupDb( boost::python::ptr(pm) );
    }

};

BOOST_PYTHON_MODULE(pyplusplus){
    { //::std::vector< std::string >
        typedef bp::class_< std::vector< std::string > > vector_less__std_scope_string__greater__exposer_t;
        vector_less__std_scope_string__greater__exposer_t vector_less__std_scope_string__greater__exposer = vector_less__std_scope_string__greater__exposer_t( "vector_less__std_scope_string__greater_" );
        bp::scope vector_less__std_scope_string__greater__scope( vector_less__std_scope_string__greater__exposer );
        vector_less__std_scope_string__greater__exposer.def( bp::vector_indexing_suite< ::std::vector< std::string >, true >() );
    }

    { //scope begin
        typedef bp::class_< std::vector< pool > > vector_less__pool__greater__exposer_t;
        vector_less__pool__greater__exposer_t vector_less__pool__greater__exposer = vector_less__pool__greater__exposer_t("vector_less__pool__greater_");
        bp::scope vector_less__pool__greater__scope( vector_less__pool__greater__exposer );
        //WARNING: the next line of code will not compile, because "::pool" does not have operator== !
        vector_less__pool__greater__exposer.def( bp::vector_indexing_suite< ::std::vector< pool > >() );
    } //scope end

    { //::std::vector< groupinfo >
        typedef bp::class_< std::vector< groupinfo > > vector_less__groupinfo__greater__exposer_t;
        vector_less__groupinfo__greater__exposer_t vector_less__groupinfo__greater__exposer = vector_less__groupinfo__greater__exposer_t( "vector_less__groupinfo__greater_" );
        bp::scope vector_less__groupinfo__greater__scope( vector_less__groupinfo__greater__exposer );
        //WARNING: the next line of code will not compile, because "::groupinfo" does not have operator== !
        vector_less__groupinfo__greater__exposer.def( bp::vector_indexing_suite< ::std::vector< groupinfo > >() );
    }

    { //scope begin
        typedef bp::class_< std::vector< filereplica > > vector_less__filereplica__greater__exposer_t;
        vector_less__filereplica__greater__exposer_t vector_less__filereplica__greater__exposer = vector_less__filereplica__greater__exposer_t("vector_less__filereplica__greater_");
        bp::scope vector_less__filereplica__greater__scope( vector_less__filereplica__greater__exposer );
        //WARNING: the next line of code will not compile, because "::filereplica" does not have operator== !
        vector_less__filereplica__greater__exposer.def( bp::vector_indexing_suite< ::std::vector< filereplica > >() );
    } //scope end

    { //scope begin
        typedef bp::class_< std::vector< dm_acl > > vector_less__dm_acl__greater__exposer_t;
        vector_less__dm_acl__greater__exposer_t vector_less__dm_acl__greater__exposer = vector_less__dm_acl__greater__exposer_t("vector_less__dm_acl__greater_");
        bp::scope vector_less__dm_acl__greater__scope( vector_less__dm_acl__greater__exposer );
        //WARNING: the next line of code will not compile, because "::dm_acl" does not have operator== !
        vector_less__dm_acl__greater__exposer.def( bp::vector_indexing_suite< ::std::vector< dm_acl > >() );
    } //scope end

    bp::class_< std::map< std::string, std::string > >("map_less__std_scope_string_comma__std_scope_string__greater_")    
        .def( bp::map_indexing_suite< ::std::map< std::string, std::string >, true >() );

    bp::class_< BaseFactory_wrapper, boost::noncopyable >( "BaseFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::BaseFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::BaseFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) );

    bp::class_< BaseInterface_wrapper, boost::noncopyable >( "BaseInterface" )    
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::BaseInterface::* )(  ) )(&::dmlite::BaseInterface::getImplId) ) )    
        .def( 
            "setSecurityContext"
            , (void ( BaseInterface_wrapper::* )( ::dmlite::SecurityContext const * ) )(&BaseInterface_wrapper::setSecurityContext)
            , ( bp::arg("ctx") ) )    
        .def( 
            "setSecurityContext"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * ))(&BaseInterface_wrapper::setSecurityContext)
            , ( bp::arg("i"), bp::arg("ctx") ) )    
        .def( 
            "setStackInstance"
            , (void ( BaseInterface_wrapper::* )( ::dmlite::StackInstance * ) )(&BaseInterface_wrapper::setStackInstance)
            , ( bp::arg("si") ) )    
        .def( 
            "setStackInstance"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::StackInstance * ))(&BaseInterface_wrapper::setStackInstance)
            , ( bp::arg("i"), bp::arg("si") ) )    
        .staticmethod( "setSecurityContext" )    
        .staticmethod( "setStackInstance" );

    bp::class_< Catalog_wrapper, bp::bases< dmlite::BaseInterface >, boost::noncopyable >( "Catalog" )    
        .def( 
            "addReplica"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::FileReplica const & ) )(&::dmlite::Catalog::addReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "changeDir"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::changeDir) )
            , ( bp::arg("path") ) )    
        .def( 
            "closeDir"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::dmlite::Directory * ) )(&::dmlite::Catalog::closeDir) )
            , ( bp::arg("dir") ) )    
        .def( 
            "create"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::mode_t ) )(&::dmlite::Catalog::create) )
            , ( bp::arg("path"), bp::arg("mode") ) )    
        .def( 
            "deleteReplica"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::FileReplica const & ) )(&::dmlite::Catalog::deleteReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "extendedStat"
            , bp::pure_virtual( (::ExtendedStat ( ::dmlite::Catalog::* )( ::std::string const &,bool ) )(&::dmlite::Catalog::extendedStat) )
            , ( bp::arg("path"), bp::arg("followSym")=(bool)(true) ) )    
        .def( 
            "getComment"
            , bp::pure_virtual( (::std::string ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::getComment) )
            , ( bp::arg("path") ) )    
        .def( 
            "getReplica"
            , bp::pure_virtual( (::FileReplica ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::getReplica) )
            , ( bp::arg("rfn") ) )    
        .def( 
            "getReplicas"
            , bp::pure_virtual( (::std::vector< filereplica > ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::getReplicas) )
            , ( bp::arg("path") ) )    
        .def( 
            "getWorkingDir"
            , bp::pure_virtual( (::std::string ( ::dmlite::Catalog::* )(  ) )(&::dmlite::Catalog::getWorkingDir) ) )    
        .def( 
            "makeDir"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::mode_t ) )(&::dmlite::Catalog::makeDir) )
            , ( bp::arg("path"), bp::arg("mode") ) )    
        .def( 
            "openDir"
            , bp::pure_virtual( (::dmlite::Directory * ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::openDir) )
            , ( bp::arg("path") )
            , bp::return_value_policy< bp::return_opaque_pointer >() )    
        .def( 
            "readDir"
            , bp::pure_virtual( (::dirent * ( ::dmlite::Catalog::* )( ::dmlite::Directory * ) )(&::dmlite::Catalog::readDir) )
            , ( bp::arg("dir") )
                // undefined call policies 
 )    
        .def( 
            "readDirx"
            , bp::pure_virtual( (::ExtendedStat * ( ::dmlite::Catalog::* )( ::dmlite::Directory * ) )(&::dmlite::Catalog::readDirx) )
            , ( bp::arg("dir") )
                // undefined call policies 
 )    
        .def( 
            "removeDir"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::removeDir) )
            , ( bp::arg("path") ) )    
        .def( 
            "rename"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::string const & ) )(&::dmlite::Catalog::rename) )
            , ( bp::arg("oldPath"), bp::arg("newPath") ) )    
        .def( 
            "setAcl"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::vector< dm_acl > const & ) )(&::dmlite::Catalog::setAcl) )
            , ( bp::arg("path"), bp::arg("acls") ) )    
        .def( 
            "setChecksum"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::string const &,::std::string const & ) )(&::dmlite::Catalog::setChecksum) )
            , ( bp::arg("path"), bp::arg("csumtype"), bp::arg("csumvalue") ) )    
        .def( 
            "setComment"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::string const & ) )(&::dmlite::Catalog::setComment) )
            , ( bp::arg("path"), bp::arg("comment") ) )    
        .def( 
            "setGuid"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::string const & ) )(&::dmlite::Catalog::setGuid) )
            , ( bp::arg("path"), bp::arg("guid") ) )    
        .def( 
            "setMode"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::mode_t ) )(&::dmlite::Catalog::setMode) )
            , ( bp::arg("path"), bp::arg("mode") ) )    
        .def( 
            "setOwner"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::uid_t,::gid_t,bool ) )(&::dmlite::Catalog::setOwner) )
            , ( bp::arg("path"), bp::arg("newUid"), bp::arg("newGid"), bp::arg("followSymLink")=(bool)(true) ) )    
        .def( 
            "setSecurityContext"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::dmlite::SecurityContext const * ) )(&::dmlite::Catalog::setSecurityContext) )
            , ( bp::arg("ctx") ) )    
        .def( 
            "setSize"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::size_t ) )(&::dmlite::Catalog::setSize) )
            , ( bp::arg("path"), bp::arg("newSize") ) )    
        .def( 
            "setStackInstance"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::dmlite::StackInstance * ) )(&::dmlite::Catalog::setStackInstance) )
            , ( bp::arg("si") ) )    
        .def( 
            "symlink"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::std::string const & ) )(&::dmlite::Catalog::symlink) )
            , ( bp::arg("oldpath"), bp::arg("newpath") ) )    
        .def( 
            "umask"
            , bp::pure_virtual( (::mode_t ( ::dmlite::Catalog::* )( ::mode_t ) )(&::dmlite::Catalog::umask) )
            , ( bp::arg("mask") ) )    
        .def( 
            "unlink"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const & ) )(&::dmlite::Catalog::unlink) )
            , ( bp::arg("path") ) )    
        .def( 
            "updateReplica"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::FileReplica const & ) )(&::dmlite::Catalog::updateReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "utime"
            , bp::pure_virtual( (void ( ::dmlite::Catalog::* )( ::std::string const &,::utimbuf const * ) )(&::dmlite::Catalog::utime) )
            , ( bp::arg("path"), bp::arg("buf") ) )    
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::BaseInterface::* )(  ) )(&::dmlite::BaseInterface::getImplId) ) )    
        .def( 
            "setSecurityContext"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * ))(&Catalog_wrapper::setSecurityContext)
            , ( bp::arg("i"), bp::arg("ctx") ) )    
        .def( 
            "setStackInstance"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::StackInstance * ))(&Catalog_wrapper::setStackInstance)
            , ( bp::arg("i"), bp::arg("si") ) )    
        .staticmethod( "setSecurityContext" )    
        .staticmethod( "setStackInstance" );

    bp::class_< CatalogFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "CatalogFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::CatalogFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::CatalogFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createCatalog"
            , (::dmlite::Catalog * (*)( ::dmlite::CatalogFactory *,::dmlite::PluginManager * ))(&CatalogFactory_wrapper::createCatalog)
            , ( bp::arg("factory"), bp::arg("pm") )
                // undefined call policies 
 )    
        .def( 
            "createCatalog"
            , (::dmlite::Catalog * ( CatalogFactory_wrapper::* )( ::dmlite::PluginManager * ) )(&CatalogFactory_wrapper::createCatalog)
            , ( bp::arg("pm") )
                // undefined call policies 
 )    
        .staticmethod( "createCatalog" );

    { //::dmlite::DmException
        typedef bp::class_< DmException_wrapper > DmException_exposer_t;
        DmException_exposer_t DmException_exposer = DmException_exposer_t( "DmException", bp::init< >() );
        bp::scope DmException_scope( DmException_exposer );
        DmException_exposer.def( bp::init< int >(( bp::arg("code") )) );
        bp::implicitly_convertible< int, dmlite::DmException >();
        DmException_exposer.def( bp::init< int, std::string const & >(( bp::arg("code"), bp::arg("string") )) );
        //DmException_exposer.def( bp::init< int, char const *, __va_list_tag * >(( bp::arg("code"), bp::arg("fmt"), bp::arg("args") )) );
        DmException_exposer.def( bp::init< dmlite::DmException const & >(( bp::arg("de") )) );
        { //::dmlite::DmException::code
        
            typedef int ( ::dmlite::DmException::*code_function_type )(  ) const;
            
            DmException_exposer.def( 
                "code"
                , code_function_type( &::dmlite::DmException::code ) );
        
        }
        { //::dmlite::DmException::setMessage
        
            //typedef void ( DmException_wrapper::*setMessage_function_type )( char const *,::__va_list_tag * ) ;
            
            DmException_exposer.def( 
                "setMessage"
                , setMessage_function_type( &DmException_wrapper::setMessage )
                , ( bp::arg("fmt"), bp::arg("args") ) );
        
        }
        { //::dmlite::DmException::what
        
            typedef char const * ( ::dmlite::DmException::*what_function_type )(  ) const;
            typedef char const * ( DmException_wrapper::*default_what_function_type )(  ) const;
            
            DmException_exposer.def( 
                "what"
                , what_function_type(&::dmlite::DmException::what)
                , default_what_function_type(&DmException_wrapper::default_what) );
        
        }
    }

    bp::class_< INode_wrapper, bp::bases< dmlite::BaseInterface >, boost::noncopyable >( "INode" )    
        .def( 
            "addReplica"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::FileReplica const & ) )(&::dmlite::INode::addReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "begin"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )(  ) )(&::dmlite::INode::begin) ) )    
        .def( 
            "closeDir"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::dmlite::IDirectory * ) )(&::dmlite::INode::closeDir) )
            , ( bp::arg("dir") ) )    
        .def( 
            "commit"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )(  ) )(&::dmlite::INode::commit) ) )    
        .def( 
            "create"
            , bp::pure_virtual( (::ExtendedStat ( ::dmlite::INode::* )( ::ino_t,::std::string const &,::uid_t,::gid_t,::mode_t,::size_t,short int,char,::std::string const &,::std::string const &,::std::string const & ) )(&::dmlite::INode::create) )
            , ( bp::arg("parent"), bp::arg("name"), bp::arg("uid"), bp::arg("gid"), bp::arg("mode"), bp::arg("size"), bp::arg("type"), bp::arg("status"), bp::arg("csumtype"), bp::arg("csumvalue"), bp::arg("acl") ) )    
        .def( 
            "deleteComment"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::deleteComment) )
            , ( bp::arg("inode") ) )    
        .def( 
            "deleteReplica"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::FileReplica const & ) )(&::dmlite::INode::deleteReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "extendedStat"
            , bp::pure_virtual( (::ExtendedStat ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::extendedStat) )
            , ( bp::arg("inode") ) )    
        .def( 
            "extendedStat"
            , bp::pure_virtual( (::ExtendedStat ( ::dmlite::INode::* )( ::ino_t,::std::string const & ) )(&::dmlite::INode::extendedStat) )
            , ( bp::arg("parent"), bp::arg("name") ) )    
        .def( 
            "extendedStat"
            , bp::pure_virtual( (::ExtendedStat ( ::dmlite::INode::* )( ::std::string const & ) )(&::dmlite::INode::extendedStat) )
            , ( bp::arg("guid") ) )    
        .def( 
            "getComment"
            , bp::pure_virtual( (::std::string ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::getComment) )
            , ( bp::arg("inode") ) )    
        .def( 
            "getReplica"
            , bp::pure_virtual( (::FileReplica ( ::dmlite::INode::* )( ::int64_t ) )(&::dmlite::INode::getReplica) )
            , ( bp::arg("rid") ) )    
        .def( 
            "getReplica"
            , bp::pure_virtual( (::FileReplica ( ::dmlite::INode::* )( ::std::string const & ) )(&::dmlite::INode::getReplica) )
            , ( bp::arg("rfn") ) )    
        .def( 
            "getReplicas"
            , bp::pure_virtual( (::std::vector< filereplica > ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::getReplicas) )
            , ( bp::arg("inode") ) )    
        .def( 
            "move"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::ino_t ) )(&::dmlite::INode::move) )
            , ( bp::arg("inode"), bp::arg("dest") ) )    
        .def( 
            "openDir"
            , bp::pure_virtual( (::dmlite::IDirectory * ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::openDir) )
            , ( bp::arg("inode") )
            , bp::return_value_policy< bp::return_opaque_pointer >() )    
        .def( 
            "readDir"
            , bp::pure_virtual( (::dirent * ( ::dmlite::INode::* )( ::dmlite::IDirectory * ) )(&::dmlite::INode::readDir) )
            , ( bp::arg("dir") )
                // undefined call policies 
 )    
        .def( 
            "readDirx"
            , bp::pure_virtual( (::ExtendedStat * ( ::dmlite::INode::* )( ::dmlite::IDirectory * ) )(&::dmlite::INode::readDirx) )
            , ( bp::arg("dir") )
                // undefined call policies 
 )    
        .def( 
            "readLink"
            , bp::pure_virtual( (::SymLink ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::readLink) )
            , ( bp::arg("inode") ) )    
        .def( 
            "rename"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::std::string const & ) )(&::dmlite::INode::rename) )
            , ( bp::arg("inode"), bp::arg("name") ) )    
        .def( 
            "rollback"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )(  ) )(&::dmlite::INode::rollback) ) )    
        .def( 
            "setChecksum"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::std::string const &,::std::string const & ) )(&::dmlite::INode::setChecksum) )
            , ( bp::arg("inode"), bp::arg("csumtype"), bp::arg("csumvalue") ) )    
        .def( 
            "setComment"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::std::string const & ) )(&::dmlite::INode::setComment) )
            , ( bp::arg("inode"), bp::arg("comment") ) )    
        .def( 
            "setGuid"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::std::string const & ) )(&::dmlite::INode::setGuid) )
            , ( bp::arg("inode"), bp::arg("guid") ) )    
        .def( 
            "setMode"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::uid_t,::gid_t,::mode_t,::std::string const & ) )(&::dmlite::INode::setMode) )
            , ( bp::arg("inode"), bp::arg("uid"), bp::arg("gid"), bp::arg("mode"), bp::arg("acl") ) )    
        .def( 
            "setSize"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::size_t ) )(&::dmlite::INode::setSize) )
            , ( bp::arg("inode"), bp::arg("size") ) )    
        .def( 
            "symlink"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::std::string const & ) )(&::dmlite::INode::symlink) )
            , ( bp::arg("inode"), bp::arg("link") ) )    
        .def( 
            "unlink"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t ) )(&::dmlite::INode::unlink) )
            , ( bp::arg("inode") ) )    
        .def( 
            "updateReplica"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::FileReplica const & ) )(&::dmlite::INode::updateReplica) )
            , ( bp::arg("replica") ) )    
        .def( 
            "utime"
            , bp::pure_virtual( (void ( ::dmlite::INode::* )( ::ino_t,::utimbuf const * ) )(&::dmlite::INode::utime) )
            , ( bp::arg("inode"), bp::arg("buf") ) )    
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::BaseInterface::* )(  ) )(&::dmlite::BaseInterface::getImplId) ) )    
        .def( 
            "setSecurityContext"
            , (void ( INode_wrapper::* )( ::dmlite::SecurityContext const * ) )(&INode_wrapper::setSecurityContext)
            , ( bp::arg("ctx") ) )    
        .def( 
            "setSecurityContext"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * ))(&INode_wrapper::setSecurityContext)
            , ( bp::arg("i"), bp::arg("ctx") ) )    
        .def( 
            "setStackInstance"
            , (void ( INode_wrapper::* )( ::dmlite::StackInstance * ) )(&INode_wrapper::setStackInstance)
            , ( bp::arg("si") ) )    
        .def( 
            "setStackInstance"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::StackInstance * ))(&INode_wrapper::setStackInstance)
            , ( bp::arg("i"), bp::arg("si") ) )    
        .staticmethod( "setSecurityContext" )    
        .staticmethod( "setStackInstance" );

    bp::class_< INodeFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "INodeFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::INodeFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::INodeFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createINode"
            , (::dmlite::INode * (*)( ::dmlite::INodeFactory *,::dmlite::PluginManager * ))(&INodeFactory_wrapper::createINode)
            , ( bp::arg("factory"), bp::arg("pm") )
                // undefined call policies 
 )    
        .def( 
            "createINode"
            , (::dmlite::INode * ( INodeFactory_wrapper::* )( ::dmlite::PluginManager * ) )(&INodeFactory_wrapper::createINode)
            , ( bp::arg("pm") )
                // undefined call policies 
 )    
        .staticmethod( "createINode" );

    bp::class_< IODriver_wrapper, bp::bases< dmlite::BaseInterface >, boost::noncopyable >( "IODriver" )    
        .def( 
            "createIOHandler"
            , bp::pure_virtual( (::dmlite::IOHandler * ( ::dmlite::IODriver::* )( ::std::string const &,int,::std::map< std::string, std::string > const & ) )(&::dmlite::IODriver::createIOHandler) )
            , ( bp::arg("pfn"), bp::arg("flags"), bp::arg("extras") )
                // undefined call policies 
 )    
        .def( 
            "pStat"
            , bp::pure_virtual( (::stat ( ::dmlite::IODriver::* )( ::std::string const & ) )(&::dmlite::IODriver::pStat) )
            , ( bp::arg("pfn") ) ) 
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::BaseInterface::* )(  ) )(&::dmlite::BaseInterface::getImplId) ) )    
        .def( 
            "setSecurityContext"
            , (void ( IODriver_wrapper::* )( ::dmlite::SecurityContext const * ) )(&IODriver_wrapper::setSecurityContext)
            , ( bp::arg("ctx") ) )    
        .def( 
            "setSecurityContext"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * ))(&IODriver_wrapper::setSecurityContext)
            , ( bp::arg("i"), bp::arg("ctx") ) )    
        .def( 
            "setStackInstance"
            , (void ( IODriver_wrapper::* )( ::dmlite::StackInstance * ) )(&IODriver_wrapper::setStackInstance)
            , ( bp::arg("si") ) )    
        .def( 
            "setStackInstance"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::StackInstance * ))(&IODriver_wrapper::setStackInstance)
            , ( bp::arg("i"), bp::arg("si") ) )    
        .staticmethod( "setSecurityContext" )    
        .staticmethod( "setStackInstance" );

    bp::class_< IOFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "IOFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::IOFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::IOFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createIODriver"
            , (::dmlite::IODriver * ( IOFactory_wrapper::* )( ::dmlite::PluginManager * ) )(&IOFactory_wrapper::createIODriver)
            , ( bp::arg("pm") )
                // undefined call policies 
 );

    bp::class_< IOHandler_wrapper, boost::noncopyable >( "IOHandler" )    
        .def( 
            "close"
            , bp::pure_virtual( (void ( ::dmlite::IOHandler::* )(  ) )(&::dmlite::IOHandler::close) ) )    
        .def( 
            "eof"
            , bp::pure_virtual( (bool ( ::dmlite::IOHandler::* )(  ) )(&::dmlite::IOHandler::eof) ) )    
        .def( 
            "flush"
            , bp::pure_virtual( (void ( ::dmlite::IOHandler::* )(  ) )(&::dmlite::IOHandler::flush) ) )    
        .def( 
            "read"
            , bp::pure_virtual( (::size_t ( ::dmlite::IOHandler::* )( char *,::size_t ) )(&::dmlite::IOHandler::read) )
            , ( bp::arg("buffer"), bp::arg("count") ) )    
        .def( 
            "seek"
            , bp::pure_virtual( (void ( ::dmlite::IOHandler::* )( long int,int ) )(&::dmlite::IOHandler::seek) )
            , ( bp::arg("offset"), bp::arg("whence") ) )    
        .def( 
            "tell"
            , bp::pure_virtual( (long int ( ::dmlite::IOHandler::* )(  ) )(&::dmlite::IOHandler::tell) ) )    
        .def( 
            "write"
            , bp::pure_virtual( (::size_t ( ::dmlite::IOHandler::* )( char const *,::size_t ) )(&::dmlite::IOHandler::write) )
            , ( bp::arg("buffer"), bp::arg("count") ) );

    bp::class_< dmlite::PluginIdCard, boost::noncopyable >( "PluginIdCard", bp::no_init )    
        .def_readonly( "ApiVersion", &dmlite::PluginIdCard::ApiVersion );

    bp::class_< dmlite::PluginManager, boost::noncopyable >( "PluginManager", bp::init< >() )    
        .def( 
            "configure"
            , (void ( ::dmlite::PluginManager::* )( ::std::string const &,::std::string const & ) )( &::dmlite::PluginManager::configure )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "getCatalogFactory"
            , (::dmlite::CatalogFactory * ( ::dmlite::PluginManager::* )(  ) )( &::dmlite::PluginManager::getCatalogFactory )
                // undefined call policies 
 )    
        .def( 
            "getINodeFactory"
            , (::dmlite::INodeFactory * ( ::dmlite::PluginManager::* )(  ) )( &::dmlite::PluginManager::getINodeFactory )
                // undefined call policies 
 )    
        .def( 
            "getIOFactory"
            , (::dmlite::IOFactory * ( ::dmlite::PluginManager::* )(  ) )( &::dmlite::PluginManager::getIOFactory )
                // undefined call policies 
 )    
        .def( 
            "getPoolDriverFactory"
            , (::dmlite::PoolDriverFactory * ( ::dmlite::PluginManager::* )( ::std::string const & ) )( &::dmlite::PluginManager::getPoolDriverFactory )
            , ( bp::arg("pooltype") )
                // undefined call policies 
 )    
        .def( 
            "getPoolManagerFactory"
            , (::dmlite::PoolManagerFactory * ( ::dmlite::PluginManager::* )(  ) )( &::dmlite::PluginManager::getPoolManagerFactory )
                // undefined call policies 
 )    
        .def( 
            "getUserGroupDbFactory"
            , (::dmlite::UserGroupDbFactory * ( ::dmlite::PluginManager::* )(  ) )( &::dmlite::PluginManager::getUserGroupDbFactory )
                // undefined call policies 
 )    
        .def( 
            "loadConfiguration"
            , (void ( ::dmlite::PluginManager::* )( ::std::string const & ) )( &::dmlite::PluginManager::loadConfiguration )
            , ( bp::arg("file") ) )    
        .def( 
            "loadPlugin"
            , (void ( ::dmlite::PluginManager::* )( ::std::string const &,::std::string const & ) )( &::dmlite::PluginManager::loadPlugin )
            , ( bp::arg("lib"), bp::arg("id") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::UserGroupDbFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::INodeFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::CatalogFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::PoolManagerFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::IOFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) )    
        .def( 
            "registerFactory"
            , (void ( ::dmlite::PluginManager::* )( ::dmlite::PoolDriverFactory * ) )( &::dmlite::PluginManager::registerFactory )
            , ( bp::arg("factory") ) );

    bp::class_< PoolDriver_wrapper, bp::bases< dmlite::BaseInterface >, boost::noncopyable >( "PoolDriver" )    
        .def( 
            "createPoolHandler"
            , bp::pure_virtual( (::dmlite::PoolHandler * ( ::dmlite::PoolDriver::* )( ::std::string const & ) )(&::dmlite::PoolDriver::createPoolHandler) )
            , ( bp::arg("poolName") )
                // undefined call policies 
 )    
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::BaseInterface::* )(  ) )(&::dmlite::BaseInterface::getImplId) ) )    
        .def( 
            "setSecurityContext"
            , (void ( PoolDriver_wrapper::* )( ::dmlite::SecurityContext const * ) )(&PoolDriver_wrapper::setSecurityContext)
            , ( bp::arg("ctx") ) )    
        .def( 
            "setSecurityContext"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * ))(&PoolDriver_wrapper::setSecurityContext)
            , ( bp::arg("i"), bp::arg("ctx") ) )    
        .def( 
            "setStackInstance"
            , (void ( PoolDriver_wrapper::* )( ::dmlite::StackInstance * ) )(&PoolDriver_wrapper::setStackInstance)
            , ( bp::arg("si") ) )    
        .def( 
            "setStackInstance"
            , (void (*)( ::dmlite::BaseInterface *,::dmlite::StackInstance * ))(&PoolDriver_wrapper::setStackInstance)
            , ( bp::arg("i"), bp::arg("si") ) )    
        .staticmethod( "setSecurityContext" )    
        .staticmethod( "setStackInstance" );

    bp::class_< PoolDriverFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "PoolDriverFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::PoolDriverFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::PoolDriverFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createPoolDriver"
            , (::dmlite::PoolDriver * ( PoolDriverFactory_wrapper::* )(  ) )(&PoolDriverFactory_wrapper::createPoolDriver)
                // undefined call policies 
 )    
        .def( 
            "implementedPool"
            , bp::pure_virtual( (::std::string ( ::dmlite::PoolDriverFactory::* )(  ) )(&::dmlite::PoolDriverFactory::implementedPool) ) );

    bp::class_< PoolHandler_wrapper, boost::noncopyable >( "PoolHandler" )    
        .def( 
            "getFreeSpace"
            , bp::pure_virtual( (::uint64_t ( ::dmlite::PoolHandler::* )(  ) )(&::dmlite::PoolHandler::getFreeSpace) ) )    
        .def( 
            "getLocation"
            , bp::pure_virtual( (::Location ( ::dmlite::PoolHandler::* )( ::FileReplica const & ) )(&::dmlite::PoolHandler::getLocation) )
            , ( bp::arg("replica") ) )    
        .def( 
            "getPoolName"
            , bp::pure_virtual( (::std::string ( ::dmlite::PoolHandler::* )(  ) )(&::dmlite::PoolHandler::getPoolName) ) )    
        .def( 
            "getPoolType"
            , bp::pure_virtual( (::std::string ( ::dmlite::PoolHandler::* )(  ) )(&::dmlite::PoolHandler::getPoolType) ) )    
        .def( 
            "getTotalSpace"
            , bp::pure_virtual( (::uint64_t ( ::dmlite::PoolHandler::* )(  ) )(&::dmlite::PoolHandler::getTotalSpace) ) )    
        .def( 
            "isAvailable"
            , bp::pure_virtual( (bool ( ::dmlite::PoolHandler::* )( bool ) )(&::dmlite::PoolHandler::isAvailable) )
            , ( bp::arg("write")=(bool)(true) ) )    
        .def( 
            "putDone"
            , bp::pure_virtual( (void ( ::dmlite::PoolHandler::* )( ::FileReplica const &,::std::map< std::string, std::string > const & ) )(&::dmlite::PoolHandler::putDone) )
            , ( bp::arg("replica"), bp::arg("extras") ) )    
        .def( 
            "putLocation"
            , bp::pure_virtual( (::Location ( ::dmlite::PoolHandler::* )( ::std::string const & ) )(&::dmlite::PoolHandler::putLocation) )
            , ( bp::arg("fn") ) )    
        .def( 
            "remove"
            , bp::pure_virtual( (void ( ::dmlite::PoolHandler::* )( ::FileReplica const & ) )(&::dmlite::PoolHandler::remove) )
            , ( bp::arg("replica") ) );

    { //::dmlite::PoolManager
        typedef bp::class_< PoolManager_wrapper, bp::bases< dmlite::BaseInterface >, boost::noncopyable > PoolManager_exposer_t;
        PoolManager_exposer_t PoolManager_exposer = PoolManager_exposer_t( "PoolManager" );
        bp::scope PoolManager_scope( PoolManager_exposer );
        bp::enum_< dmlite::PoolManager::PoolAvailability>("PoolAvailability")
            .value("kAny", dmlite::PoolManager::kAny)
            .value("kNone", dmlite::PoolManager::kNone)
            .value("kForRead", dmlite::PoolManager::kForRead)
            .value("kForWrite", dmlite::PoolManager::kForWrite)
            .value("kForBoth", dmlite::PoolManager::kForBoth)
            .export_values()
            ;
        { //::dmlite::PoolManager::doneWriting
        
            typedef void ( ::dmlite::PoolManager::*doneWriting_function_type )( ::std::string const &,::std::string const &,::std::map<std::basic_string<char, std::char_traits<char>, std::allocator<char> >,std::basic_string<char, std::char_traits<char>, std::allocator<char> >,std::less<std::basic_string<char, std::char_traits<char>, std::allocator<char> > >,std::allocator<std::pair<const std::basic_string<char, std::char_traits<char>, std::allocator<char> >, std::basic_string<char, std::char_traits<char>, std::allocator<char> > > > > const & ) ;
            
            PoolManager_exposer.def( 
                "doneWriting"
                , bp::pure_virtual( doneWriting_function_type(&::dmlite::PoolManager::doneWriting) )
                , ( bp::arg("host"), bp::arg("rfn"), bp::arg("params") ) );
        
        }
        { //::dmlite::PoolManager::getPool
        
            typedef ::Pool ( ::dmlite::PoolManager::*getPool_function_type )( ::std::string const & ) ;
            
            PoolManager_exposer.def( 
                "getPool"
                , bp::pure_virtual( getPool_function_type(&::dmlite::PoolManager::getPool) )
                , ( bp::arg("poolname") ) );
        
        }
        { //::dmlite::PoolManager::getPoolMetadata
        
            typedef ::dmlite::PoolMetadata * ( ::dmlite::PoolManager::*getPoolMetadata_function_type )( ::std::string const & ) ;
            
            PoolManager_exposer.def( 
                "getPoolMetadata"
                , bp::pure_virtual( getPoolMetadata_function_type(&::dmlite::PoolManager::getPoolMetadata) )
                , ( bp::arg("poolName") )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::PoolManager::getPools
        
            typedef ::std::vector<pool,std::allocator<pool> > ( ::dmlite::PoolManager::*getPools_function_type )( ::dmlite::PoolManager::PoolAvailability ) ;
            
            PoolManager_exposer.def( 
                "getPools"
                , bp::pure_virtual( getPools_function_type(&::dmlite::PoolManager::getPools) )
                , ( bp::arg("availability")=::dmlite::PoolManager::kAny ) );
        
        }
        { //::dmlite::PoolManager::whereToRead
        
            typedef ::Location ( ::dmlite::PoolManager::*whereToRead_function_type )( ::std::string const & ) ;
            
            PoolManager_exposer.def( 
                "whereToRead"
                , bp::pure_virtual( whereToRead_function_type(&::dmlite::PoolManager::whereToRead) )
                , ( bp::arg("path") ) );
        
        }
        { //::dmlite::PoolManager::whereToWrite
        
            typedef ::Location ( ::dmlite::PoolManager::*whereToWrite_function_type )( ::std::string const & ) ;
            
            PoolManager_exposer.def( 
                "whereToWrite"
                , bp::pure_virtual( whereToWrite_function_type(&::dmlite::PoolManager::whereToWrite) )
                , ( bp::arg("path") ) );
        
        }
        { //::dmlite::BaseInterface::getImplId
        
            typedef ::std::string ( ::dmlite::BaseInterface::*getImplId_function_type )(  ) ;
            
            PoolManager_exposer.def( 
                "getImplId"
                , bp::pure_virtual( getImplId_function_type(&::dmlite::BaseInterface::getImplId) ) );
        
        }
        { //::dmlite::BaseInterface::setSecurityContext
        
            typedef void ( PoolManager_wrapper::*setSecurityContext_function_type )( ::dmlite::SecurityContext const * ) ;
            
            PoolManager_exposer.def( 
                "setSecurityContext"
                , setSecurityContext_function_type( &PoolManager_wrapper::setSecurityContext )
                , ( bp::arg("ctx") ) );
        
        }
        { //::dmlite::BaseInterface::setSecurityContext
        
            typedef void ( *setSecurityContext_function_type )( ::dmlite::BaseInterface *,::dmlite::SecurityContext const * );
            
            PoolManager_exposer.def( 
                "setSecurityContext"
                , setSecurityContext_function_type( &PoolManager_wrapper::setSecurityContext )
                , ( bp::arg("i"), bp::arg("ctx") ) );
        
        }
        { //::dmlite::BaseInterface::setStackInstance
        
            typedef void ( PoolManager_wrapper::*setStackInstance_function_type )( ::dmlite::StackInstance * ) ;
            
            PoolManager_exposer.def( 
                "setStackInstance"
                , setStackInstance_function_type( &PoolManager_wrapper::setStackInstance )
                , ( bp::arg("si") ) );
        
        }
        { //::dmlite::BaseInterface::setStackInstance
        
            typedef void ( *setStackInstance_function_type )( ::dmlite::BaseInterface *,::dmlite::StackInstance * );
            
            PoolManager_exposer.def( 
                "setStackInstance"
                , setStackInstance_function_type( &PoolManager_wrapper::setStackInstance )
                , ( bp::arg("i"), bp::arg("si") ) );
        
        }
        PoolManager_exposer.staticmethod( "setSecurityContext" );
        PoolManager_exposer.staticmethod( "setStackInstance" );
    }

    bp::class_< PoolManagerFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "PoolManagerFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::PoolManagerFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::PoolManagerFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createPoolManager"
            , (::dmlite::PoolManager * (*)( ::dmlite::PoolManagerFactory *,::dmlite::PluginManager * ))(&PoolManagerFactory_wrapper::createPoolManager)
            , ( bp::arg("factory"), bp::arg("pm") )
                // undefined call policies 
 )    
        .def( 
            "createPoolManager"
            , (::dmlite::PoolManager * ( PoolManagerFactory_wrapper::* )( ::dmlite::PluginManager * ) )(&PoolManagerFactory_wrapper::createPoolManager)
            , ( bp::arg("pm") )
                // undefined call policies 
 )    
        .staticmethod( "createPoolManager" );

    bp::class_< PoolMetadata_wrapper, boost::noncopyable >( "PoolMetadata" )    
        .def( 
            "getInt"
            , bp::pure_virtual( (int ( ::dmlite::PoolMetadata::* )( ::std::string const & ) )(&::dmlite::PoolMetadata::getInt) )
            , ( bp::arg("field") ) )    
        .def( 
            "getString"
            , bp::pure_virtual( (::std::string ( ::dmlite::PoolMetadata::* )( ::std::string const & ) )(&::dmlite::PoolMetadata::getString) )
            , ( bp::arg("field") ) );

    { //::dmlite::SecurityContext
        typedef bp::class_< dmlite::SecurityContext > SecurityContext_exposer_t;
        SecurityContext_exposer_t SecurityContext_exposer = SecurityContext_exposer_t( "SecurityContext", bp::init< >() );
        bp::scope SecurityContext_scope( SecurityContext_exposer );
        SecurityContext_exposer.def( bp::init< dmlite::SecurityCredentials const & >(( bp::arg("arg0") )) );
        bp::implicitly_convertible< dmlite::SecurityCredentials const &, dmlite::SecurityContext >();
        SecurityContext_exposer.def( bp::init< dmlite::SecurityCredentials const &, UserInfo const &, std::vector< groupinfo > const & >(( bp::arg("arg0"), bp::arg("arg1"), bp::arg("arg2") )) );
        { //::dmlite::SecurityContext::getCredentials
        
            typedef ::dmlite::SecurityCredentials const & ( ::dmlite::SecurityContext::*getCredentials_function_type )(  ) const;
            
            SecurityContext_exposer.def( 
                "getCredentials"
                , getCredentials_function_type( &::dmlite::SecurityContext::getCredentials )
                , bp::return_value_policy< bp::copy_const_reference >() );
        
        }
        { //::dmlite::SecurityContext::getGroup
        
            typedef ::GroupInfo const & ( ::dmlite::SecurityContext::*getGroup_function_type )( unsigned int ) const;
            
            SecurityContext_exposer.def( 
                "getGroup"
                , getGroup_function_type( &::dmlite::SecurityContext::getGroup )
                , ( bp::arg("idx")=(unsigned int)(0) )
                , bp::return_value_policy< bp::copy_const_reference >() );
        
        }
        { //::dmlite::SecurityContext::getGroup
        
            typedef ::GroupInfo & ( ::dmlite::SecurityContext::*getGroup_function_type )( unsigned int ) ;
            
            SecurityContext_exposer.def( 
                "getGroup"
                , getGroup_function_type( &::dmlite::SecurityContext::getGroup )
                , ( bp::arg("idx")=(unsigned int)(0) )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::SecurityContext::getUser
        
            typedef ::UserInfo const & ( ::dmlite::SecurityContext::*getUser_function_type )(  ) const;
            
            SecurityContext_exposer.def( 
                "getUser"
                , getUser_function_type( &::dmlite::SecurityContext::getUser )
                , bp::return_value_policy< bp::copy_const_reference >() );
        
        }
        { //::dmlite::SecurityContext::getUser
        
            typedef ::UserInfo & ( ::dmlite::SecurityContext::*getUser_function_type )(  ) ;
            
            SecurityContext_exposer.def( 
                "getUser"
                , getUser_function_type( &::dmlite::SecurityContext::getUser )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::SecurityContext::groupCount
        
            typedef unsigned int ( ::dmlite::SecurityContext::*groupCount_function_type )(  ) const;
            
            SecurityContext_exposer.def( 
                "groupCount"
                , groupCount_function_type( &::dmlite::SecurityContext::groupCount ) );
        
        }
        { //::dmlite::SecurityContext::hasGroup
        
            typedef bool ( ::dmlite::SecurityContext::*hasGroup_function_type )( ::gid_t ) const;
            
            SecurityContext_exposer.def( 
                "hasGroup"
                , hasGroup_function_type( &::dmlite::SecurityContext::hasGroup )
                , ( bp::arg("gid") ) );
        
        }
        { //::dmlite::SecurityContext::resizeGroup
        
            typedef void ( ::dmlite::SecurityContext::*resizeGroup_function_type )( unsigned int ) ;
            
            SecurityContext_exposer.def( 
                "resizeGroup"
                , resizeGroup_function_type( &::dmlite::SecurityContext::resizeGroup )
                , ( bp::arg("size") ) );
        
        }
        { //::dmlite::SecurityContext::setCredentials
        
            typedef void ( ::dmlite::SecurityContext::*setCredentials_function_type )( ::dmlite::SecurityCredentials const & ) ;
            
            SecurityContext_exposer.def( 
                "setCredentials"
                , setCredentials_function_type( &::dmlite::SecurityContext::setCredentials )
                , ( bp::arg("arg0") ) );
        
        }
    }

    { //::dmlite::SecurityCredentials
        typedef bp::class_< dmlite::SecurityCredentials > SecurityCredentials_exposer_t;
        SecurityCredentials_exposer_t SecurityCredentials_exposer = SecurityCredentials_exposer_t( "SecurityCredentials", bp::init< >() );
        bp::scope SecurityCredentials_scope( SecurityCredentials_exposer );
        SecurityCredentials_exposer.def( bp::init< Credentials const & >(( bp::arg("arg0") )) );
        bp::implicitly_convertible< Credentials const &, dmlite::SecurityCredentials >();
        SecurityCredentials_exposer.def( bp::init< dmlite::SecurityCredentials const & >(( bp::arg("arg0") )) );
        { //::dmlite::SecurityCredentials::getClientName
        
            typedef ::std::string ( ::dmlite::SecurityCredentials::*getClientName_function_type )(  ) const;
            
            SecurityCredentials_exposer.def( 
                "getClientName"
                , getClientName_function_type( &::dmlite::SecurityCredentials::getClientName ) );
        
        }
        { //::dmlite::SecurityCredentials::getFqans
        
            typedef ::std::vector< std::string > const & ( ::dmlite::SecurityCredentials::*getFqans_function_type )(  ) const;
            
            SecurityCredentials_exposer.def( 
                "getFqans"
                , getFqans_function_type( &::dmlite::SecurityCredentials::getFqans )
                , bp::return_value_policy< bp::copy_const_reference >() );
        
        }
        { //::dmlite::SecurityCredentials::getRemoteAddress
        
            typedef ::std::string ( ::dmlite::SecurityCredentials::*getRemoteAddress_function_type )(  ) const;
            
            SecurityCredentials_exposer.def( 
                "getRemoteAddress"
                , getRemoteAddress_function_type( &::dmlite::SecurityCredentials::getRemoteAddress ) );
        
        }
        { //::dmlite::SecurityCredentials::getSecurityMechanism
        
            typedef ::std::string ( ::dmlite::SecurityCredentials::*getSecurityMechanism_function_type )(  ) const;
            
            SecurityCredentials_exposer.def( 
                "getSecurityMechanism"
                , getSecurityMechanism_function_type( &::dmlite::SecurityCredentials::getSecurityMechanism ) );
        
        }
        { //::dmlite::SecurityCredentials::getSessionId
        
            typedef ::std::string ( ::dmlite::SecurityCredentials::*getSessionId_function_type )(  ) const;
            
            SecurityCredentials_exposer.def( 
                "getSessionId"
                , getSessionId_function_type( &::dmlite::SecurityCredentials::getSessionId ) );
        
        }
        { //::dmlite::SecurityCredentials::operator=
        
            typedef ::dmlite::SecurityCredentials const & ( ::dmlite::SecurityCredentials::*assign_function_type )( ::dmlite::SecurityCredentials const & ) ;
            
            SecurityCredentials_exposer.def( 
                "assign"
                , assign_function_type( &::dmlite::SecurityCredentials::operator= )
                , ( bp::arg("arg0") )
                , bp::return_value_policy< bp::copy_const_reference >() );
        
        }
    }

    { //::dmlite::StackInstance
        typedef bp::class_< dmlite::StackInstance > StackInstance_exposer_t;
        StackInstance_exposer_t StackInstance_exposer = StackInstance_exposer_t( "StackInstance", bp::init< dmlite::PluginManager * >(( bp::arg("pm") )) );
        bp::scope StackInstance_scope( StackInstance_exposer );
        bp::implicitly_convertible< dmlite::PluginManager *, dmlite::StackInstance >();
        { //::dmlite::StackInstance::erase
        
            typedef void ( ::dmlite::StackInstance::*erase_function_type )( ::std::string const & ) ;
            
            StackInstance_exposer.def( 
                "erase"
                , erase_function_type( &::dmlite::StackInstance::erase )
                , ( bp::arg("key") ) );
        
        }
        { //::dmlite::StackInstance::get
        
            typedef ::Value ( ::dmlite::StackInstance::*get_function_type )( ::std::string const & ) ;
            
            StackInstance_exposer.def( 
                "get"
                , get_function_type( &::dmlite::StackInstance::get )
                , ( bp::arg("key") ) );
        
        }
        { //::dmlite::StackInstance::getCatalog
        
            typedef ::dmlite::Catalog * ( ::dmlite::StackInstance::*getCatalog_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getCatalog"
                , getCatalog_function_type( &::dmlite::StackInstance::getCatalog )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getINode
        
            typedef ::dmlite::INode * ( ::dmlite::StackInstance::*getINode_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getINode"
                , getINode_function_type( &::dmlite::StackInstance::getINode )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getIODriver
        
            typedef ::dmlite::IODriver * ( ::dmlite::StackInstance::*getIODriver_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getIODriver"
                , getIODriver_function_type( &::dmlite::StackInstance::getIODriver )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getPluginManager
        
            typedef ::dmlite::PluginManager * ( ::dmlite::StackInstance::*getPluginManager_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getPluginManager"
                , getPluginManager_function_type( &::dmlite::StackInstance::getPluginManager )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getPoolDriver
        
            typedef ::dmlite::PoolDriver * ( ::dmlite::StackInstance::*getPoolDriver_function_type )( ::std::string const & ) ;
            
            StackInstance_exposer.def( 
                "getPoolDriver"
                , getPoolDriver_function_type( &::dmlite::StackInstance::getPoolDriver )
                , ( bp::arg("poolType") )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getPoolManager
        
            typedef ::dmlite::PoolManager * ( ::dmlite::StackInstance::*getPoolManager_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getPoolManager"
                , getPoolManager_function_type( &::dmlite::StackInstance::getPoolManager )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getSecurityContext
        
            typedef ::dmlite::SecurityContext const * ( ::dmlite::StackInstance::*getSecurityContext_function_type )(  ) const;
            
            StackInstance_exposer.def( 
                "getSecurityContext"
                , getSecurityContext_function_type( &::dmlite::StackInstance::getSecurityContext )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::getUserGroupDb
        
            typedef ::dmlite::UserGroupDb * ( ::dmlite::StackInstance::*getUserGroupDb_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "getUserGroupDb"
                , getUserGroupDb_function_type( &::dmlite::StackInstance::getUserGroupDb )
                    // undefined call policies 
 );
        
        }
        { //::dmlite::StackInstance::isTherePoolManager
        
            typedef bool ( ::dmlite::StackInstance::*isTherePoolManager_function_type )(  ) ;
            
            StackInstance_exposer.def( 
                "isTherePoolManager"
                , isTherePoolManager_function_type( &::dmlite::StackInstance::isTherePoolManager ) );
        
        }
        { //::dmlite::StackInstance::set
        
            typedef void ( ::dmlite::StackInstance::*set_function_type )( ::std::string const &,::Value & ) ;
            
            StackInstance_exposer.def( 
                "set"
                , set_function_type( &::dmlite::StackInstance::set )
                , ( bp::arg("key"), bp::arg("value") ) );
        
        }
        { //::dmlite::StackInstance::setSecurityContext
        
            typedef void ( ::dmlite::StackInstance::*setSecurityContext_function_type )( ::dmlite::SecurityContext const & ) ;
            
            StackInstance_exposer.def( 
                "setSecurityContext"
                , setSecurityContext_function_type( &::dmlite::StackInstance::setSecurityContext )
                , ( bp::arg("ctx") ) );
        
        }
        { //::dmlite::StackInstance::setSecurityCredentials
        
            typedef void ( ::dmlite::StackInstance::*setSecurityCredentials_function_type )( ::dmlite::SecurityCredentials const & ) ;
            
            StackInstance_exposer.def( 
                "setSecurityCredentials"
                , setSecurityCredentials_function_type( &::dmlite::StackInstance::setSecurityCredentials )
                , ( bp::arg("cred") ) );
        
        }
    }

    bp::class_< UserGroupDb_wrapper, boost::noncopyable >( "UserGroupDb" )    
        .def( 
            "createSecurityContext"
            , bp::pure_virtual( (::dmlite::SecurityContext * ( ::dmlite::UserGroupDb::* )( ::dmlite::SecurityCredentials const & ) )(&::dmlite::UserGroupDb::createSecurityContext) )
            , ( bp::arg("cred") )
                // undefined call policies 
 )    
        .def( 
            "getGroup"
            , bp::pure_virtual( (::GroupInfo ( ::dmlite::UserGroupDb::* )( ::std::string const & ) )(&::dmlite::UserGroupDb::getGroup) )
            , ( bp::arg("groupName") ) )    
        .def( 
            "getIdMap"
            , bp::pure_virtual( (void ( ::dmlite::UserGroupDb::* )( ::std::string const &,::std::vector< std::string > const &,::UserInfo *,::std::vector< groupinfo > * ) )(&::dmlite::UserGroupDb::getIdMap) )
            , ( bp::arg("userName"), bp::arg("groupNames"), bp::arg("user"), bp::arg("groups") ) )    
        .def( 
            "getImplId"
            , bp::pure_virtual( (::std::string ( ::dmlite::UserGroupDb::* )(  ) )(&::dmlite::UserGroupDb::getImplId) ) )    
        .def( 
            "getUser"
            , bp::pure_virtual( (::UserInfo ( ::dmlite::UserGroupDb::* )( ::std::string const & ) )(&::dmlite::UserGroupDb::getUser) )
            , ( bp::arg("userName") ) )    
        .def( 
            "newGroup"
            , bp::pure_virtual( (::GroupInfo ( ::dmlite::UserGroupDb::* )( ::std::string const & ) )(&::dmlite::UserGroupDb::newGroup) )
            , ( bp::arg("gname") ) )    
        .def( 
            "newUser"
            , bp::pure_virtual( (::UserInfo ( ::dmlite::UserGroupDb::* )( ::std::string const &,::std::string const & ) )(&::dmlite::UserGroupDb::newUser) )
            , ( bp::arg("uname"), bp::arg("ca") ) );

    bp::class_< UserGroupDbFactory_wrapper, bp::bases< dmlite::BaseFactory >, boost::noncopyable >( "UserGroupDbFactory" )    
        .def( 
            "configure"
            , bp::pure_virtual( (void ( ::dmlite::UserGroupDbFactory::* )( ::std::string const &,::std::string const & ) )(&::dmlite::UserGroupDbFactory::configure) )
            , ( bp::arg("key"), bp::arg("value") ) )    
        .def( 
            "createUserGroupDb"
            , (::dmlite::UserGroupDb * (*)( ::dmlite::UserGroupDbFactory *,::dmlite::PluginManager * ))(&UserGroupDbFactory_wrapper::createUserGroupDb)
            , ( bp::arg("factory"), bp::arg("pm") )
                // undefined call policies 
 )    
        .def( 
            "createUserGroupDb"
            , (::dmlite::UserGroupDb * ( UserGroupDbFactory_wrapper::* )( ::dmlite::PluginManager * ) )(&UserGroupDbFactory_wrapper::createUserGroupDb)
            , ( bp::arg("pm") )
                // undefined call policies 
 )    
        .staticmethod( "createUserGroupDb" );

    bp::scope().attr("API_VERSION") = dmlite::API_VERSION;
}*/
