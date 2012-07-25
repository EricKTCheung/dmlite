/// @file  plugin_mock.cpp
/// @brief Mock plugin for the C tests.
#include <dirent.h>
#include <errno.h>
#include <unistd.h>
#include <dmlite/cpp/dmlite.h>
#include "plugin_mock.h"
#include "dmlite/cpp/utils/urls.h"

using namespace dmlite;



MockCatalog::MockCatalog(): DummyCatalog(NULL)
{
  // Nothing
}



MockCatalog::~MockCatalog()
{
  // Nothing
}



std::string MockCatalog::getImplId() const throw ()
{
  return std::string("MockCatalog");
}



void MockCatalog::changeDir(const std::string& path) throw (DmException)
{
  chdir(path.c_str());
}



std::string MockCatalog::getWorkingDir() throw (DmException)
{
  char buffer[512];
  ::getcwd(buffer, sizeof(buffer));
  return std::string(buffer);
}



ExtendedStat MockCatalog::extendedStat(const std::string& path, bool) throw (DmException)
{
  ExtendedStat sx;
  
  if (stat(path.c_str(), &sx.stat) != 0)
    throw DmException(DM_NO_SUCH_FILE, "Could not stat %s", path.c_str());
  
  std::vector<std::string> components = Url::splitPath(path);
  sx.name = components.back();
  sx["easter"] = std::string("egg");
  
  return sx;
}



Directory* MockCatalog::openDir(const std::string& path) throw (DmException)
{
  MockDirectory *md;
  
  md = new MockDirectory();
  
  md->d = opendir(path.c_str());
  if (md->d == NULL) {
    delete md;
    throw DmException(DM_NO_SUCH_FILE, "Could not open %s", path.c_str());
  }
  memset(&md->holder.stat, 0, sizeof(struct stat));
  return (Directory*)md;
}



void MockCatalog::closeDir(Directory* d) throw (DmException)
{
  MockDirectory *md = (MockDirectory*)d;
  closedir(md->d);
  delete md;
}



ExtendedStat* MockCatalog::readDirx(Directory* d) throw (DmException)
{
  MockDirectory *md = (MockDirectory*)d;
  
  errno = 0;
  struct dirent* entry = readdir(md->d);
  if (entry == NULL) {
    if (errno != 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not read: %d", errno);
    return NULL;
  }
  
  md->holder.clear();
  md->holder.name = entry->d_name;
  md->holder["easter"] = std::string("egg");
  
  return &(md->holder);
}



MockPoolManager::MockPoolManager(): DummyPoolManager(NULL)
{
  // Nothing
}



MockPoolManager::~MockPoolManager()
{
  // Nothing
}



std::string MockPoolManager::getImplId() const throw()
{
  return std::string("MockPoolManager");
}



std::vector<Pool> MockPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  Pool p;
  
  p.name = "hardcoded";
  p.type = "mock";
  
  p["extra"] = std::string("something");
  
  return std::vector<Pool>(1, p);
}



Location MockPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  Location loc;
  Chunk    chunk;
  
  chunk.host   = "host1.cern.ch";
  chunk.path   = "/storage/chunk01";
  chunk.offset = 0;
  chunk.size   = 100;
  
  loc.push_back(chunk);
  
  chunk.host   = "host2.cern.ch";
  chunk.path   = "/storage/chunk02";
  chunk.offset = 101;
  chunk.size   =  50;
  chunk["token"] = std::string("123456789");
  
  loc.push_back(chunk);
  
  return loc;
}



Location MockPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  Chunk chunk;
  
  chunk.host = "host1.cern.ch";
  chunk.path   = "/storage/chunk01";
  chunk.offset = 0;
  chunk.size   = 0;
  
  chunk["token"] = std::string("987654321");
  
  return Location(1, chunk);
}



void MockPoolManager::doneWriting(const std::string& host, const std::string& rfn,
                                  const Extensible& params) throw (DmException)
{
  if (params.getString("token") != "987654321")
    throw DmException(DM_FORBIDDEN, "Invalid token");
  if (host != "host1.cern.ch")
    throw DmException(DM_INVALID_VALUE, "Invalid host");
  if (rfn != "/storage/chunk01")
    throw DmException(DM_INVALID_VALUE, "Invalid rfn");
}



MockIOHandler::MockIOHandler(): p(0), closed(false)
{
  char c = 'a';
  for (unsigned i = 0; i < sizeof(content); ++i) {
    content[i] = c++;
    if (c > 'z') c = 'a';
  }
}



MockIOHandler::~MockIOHandler()
{
  // Nothing
}



void MockIOHandler::close(void) throw (DmException)
{
  closed = true;
}


size_t MockIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  if (closed) throw DmException(DM_INTERNAL_ERROR, "Can not read after closing");
  
  size_t i;
  for (i = 0; i < count && p < (off_t)sizeof(content); ++i, ++p) {
    buffer[i] = content[p];
  }
  
  return i;
}



size_t MockIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  if (closed) throw DmException(DM_INTERNAL_ERROR, "Can not write after closing");
  
  size_t i;
  for (i = 0; i < count && p < (off_t)sizeof(content); ++i, ++p) {
    content[p] = buffer[i];
  }
  
  return i;
}



void MockIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
  if (closed) throw DmException(DM_INTERNAL_ERROR, "Can not seek after closing");
  switch (whence) {
    case kSet:
      p = offset;
      break;
    case kEnd:
      p = p  + offset;
      break;
    default:
      p += offset;
  }
  
  if (p < 0) p = 0;
}



off_t MockIOHandler::tell (void) throw (DmException)
{
  if (closed) throw DmException(DM_INTERNAL_ERROR, "Can not tell after closing");
  return p;
}



void MockIOHandler::flush(void) throw (DmException)
{
  if (closed) throw DmException(DM_INTERNAL_ERROR, "Can not flush after closing");
}



bool MockIOHandler::eof(void) throw (DmException)
{
  return closed || p > (off_t)sizeof(content);
}



MockIODriver::~MockIODriver()
{
  // Nothing
}



std::string MockIODriver::getImplId() const throw()
{
  return std::string("MockIODriver");
}



void MockIODriver::setStackInstance(StackInstance*) throw (DmException)
{
  // Ignore
}



void MockIODriver::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // Ignore
}



IOHandler* MockIODriver::createIOHandler(const std::string& pfn, OpenMode flags,
                                         const Extensible& extras) throw (DmException)
{
  // Only one recognised
  if (pfn != "/file")
    throw DmException(DM_NO_SUCH_FILE, "File  %s not found", pfn.c_str());
  
  // Check token
  switch (flags) {
    case kReadOnly:
      if (extras.getString("token") != "123456789")
        throw DmException(DM_FORBIDDEN, "Invalid token for reading");
      break;
    case kWriteOnly: case kReadAndWrite:
      if (extras.getString("token") != "987654321")
        throw DmException(DM_FORBIDDEN, "Invalid token for writing");
      break;
    default:
      throw DmException(DM_INVALID_VALUE, "Invalid value for the flags");
  }
  
  return new MockIOHandler();
}



MockFactory::~MockFactory()
{
  // Nothing
}



std::string MockFactory::implementedPool() throw ()
{
  return std::string("mock");
}



void MockFactory::configure(const std::string& key, const std::string&) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Unknown parameter %s", key.c_str());
}



Catalog* MockFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new MockCatalog();
}



IODriver* MockFactory::createIODriver(PluginManager*) throw (DmException)
{
  return new MockIODriver();
}



PoolManager* MockFactory::createPoolManager(PluginManager*) throw (DmException)
{
  return new MockPoolManager();
}



void registerMock(PluginManager* pm)
{
  MockFactory* factory = new MockFactory();
  pm->registerCatalogFactory(factory);
  pm->registerIOFactory(factory);
  pm->registerPoolManagerFactory(factory);
}



PluginIdCard plugin_mock = {
  PLUGIN_ID_HEADER,
  registerMock
};
