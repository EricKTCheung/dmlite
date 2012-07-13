/// @file    plugins/hadoop/Hadoop.h
/// @brief   hadoop plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include "Hadoop.h"

using namespace dmlite;



HadoopIOHandler::HadoopIOHandler(HadoopIODriver* driver, const std::string& uri, int openmode) throw (DmException):
  driver(driver), path(uri)
{
  // Try to open the hdfs file, map the errno to the DmException otherwise
  this->file = hdfsOpenFile(driver->fs, uri.c_str(), openmode, 0, 0, 0);
  if (!this->file) {
    switch(errno) {
      case ENOENT:
        throw DmException(DM_NO_SUCH_FILE, "File %s does not exist in the FS", uri.c_str());
      case EPERM:
        throw DmException(DM_BAD_OPERATION, "You don't have to access %s", uri.c_str());
      default:
        throw DmException(DM_INTERNAL_ERROR, "Could not open the file %s (errno %d)", uri.c_str(), errno); 
    }
  }

  this->isEof = false;
}



HadoopIOHandler::~HadoopIOHandler()
{
  // Close the file if its still open
  if(this->file)
    hdfsCloseFile(this->driver->fs, this->file);
}



void HadoopIOHandler::close(void) throw (DmException)
{
 // Close the file if its open
  if(this->file)
    hdfsCloseFile(this->driver->fs, this->file);
  this->file = 0;
}



size_t HadoopIOHandler::read(char* buffer, size_t count) throw (DmException)
{
	size_t bytes_read = hdfsRead(this->driver->fs, this->file, buffer, count);
 
  // EOF flag is returned if the number of bytes read is lesser than the requested
	if (bytes_read < count)
		this->isEof = true;

	return bytes_read;
}



// Write a chunk of a file in a Hadoop FS
size_t HadoopIOHandler::write(const char* buffer, size_t count) throw (DmException){
	return hdfsWrite(this->driver->fs, this->file, buffer, count);
}



// Position the reader pointer to the desired offset
void HadoopIOHandler::seek(long offset, int whence) throw (DmException){

	long positionToSet = 0;	

	// Whence described from where the offset has to be set (begin, current or end)
	switch(whence)
	{
    case SEEK_SET:
			positionToSet = offset;
			break;	
    case SEEK_CUR:
			positionToSet = (this->tell() + offset);
			break;
    case SEEK_END:
			positionToSet = (hdfsAvailable(this->driver->fs, this->file) - offset);
			break;
		default:
                  break;
	}

	hdfsSeek(this->driver->fs, this->file, positionToSet);
}



long HadoopIOHandler::tell(void) throw (DmException){
	return hdfsTell(this->driver->fs, this->file);
}



void HadoopIOHandler::flush(void) throw (DmException){
	hdfsFlush(this->driver->fs, this->file);
}



bool HadoopIOHandler::eof(void) throw (DmException){
	return this->isEof;
}



// Hadoop pool handling
HadoopPoolDriver::HadoopPoolDriver() throw (DmException):
            stack(0x00)
{
  // Nothing
}



HadoopPoolDriver::~HadoopPoolDriver()
{
  // Nothing
}



void HadoopPoolDriver::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // TODO
}



void HadoopPoolDriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack = si;
}



PoolHandler* HadoopPoolDriver::createPoolHandler(const std::string& poolName) throw (DmException)
{
  PoolMetadata* meta = this->stack->getPoolManager()->getPoolMetadata(poolName);
  
  std::string host  = meta->getString("hostname");
  std::string uname = meta->getString("username");
  int         port  = meta->getInt("port");
  
  hdfsFS fs = hdfsConnectAsUser(host.c_str(),
                                port,
                                uname.c_str());
  delete meta;
  
  if (fs == 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not create a HadoopPoolDriver: cannot connect to Hadoop");
  
  return new HadoopPoolHandler(poolName, fs, this->stack);
}



HadoopPoolHandler::HadoopPoolHandler(const std::string& poolName, hdfsFS fs, StackInstance* si):
      fs(fs), poolName(poolName), stack(si)
{
  // Nothing
}



HadoopPoolHandler::~HadoopPoolHandler()
{
  hdfsDisconnect(this->fs);
}



std::string HadoopPoolHandler::getPoolType(void) throw (DmException)
{
  return "hadoop";
}



std::string HadoopPoolHandler::getPoolName(void) throw (DmException)
{
  return this->poolName;
}



uint64_t HadoopPoolHandler::getTotalSpace(void) throw (DmException)
{
  tOffset total = hdfsGetCapacity(this->fs);
  if (total < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the total capacity of %s", this->poolName.c_str());
  return total;
}



uint64_t HadoopPoolHandler::getUsedSpace(void) throw (DmException)
{
  tOffset used = hdfsGetUsed(this->fs);
  if (used < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the free space of %s", this->poolName.c_str());

  return used;
}



uint64_t HadoopPoolHandler::getFreeSpace(void) throw (DmException)
{
  return this->getTotalSpace() - this->getUsedSpace();
}



bool HadoopPoolHandler::isAvailable(bool write) throw (DmException)
{
  // TODO
  return true;
}



bool HadoopPoolHandler::replicaAvailable(const FileReplica& replica) throw (DmException)
{
  if(hdfsExists(this->fs, replica.rfn) == 0)
    return true;
  return false;
}



Location HadoopPoolHandler::getLocation(const FileReplica& replica) throw (DmException)
{
  // To be done
//  throw DmException(DM_NOT_IMPLEMENTED, "hadoop::getLocation");
  std::vector<std::string> datanodes;
  if(hdfsExists(this->fs, replica.rfn) == 0){
    char*** hosts = hdfsGetHosts(this->fs, replica.rfn, 0, 1);
    if(hosts){
      int i=0;
      while(hosts[i]) {
        int j = 0;
        while(hosts[i][j]) {
          datanodes.push_back(std::string(hosts[i][j]));
          ++j;
        }
        ++i;
      }
      hdfsFreeHosts(hosts);
    }
  }
  
  // Beware! If the file size is 0, no host will be returned
  // Remit to the name node (for instance)
  if (datanodes.size() == 0) {
    throw DmException(DM_NO_REPLICAS, "No replicas found on Hadoop for %s", replica.rfn);
  }

  // Pick a location
  srand(time(NULL));
  unsigned i = rand() % datanodes.size();
  return Location(datanodes[i].c_str(), replica.rfn,
                  this->replicaAvailable(replica),
                  0);
}



void HadoopPoolHandler::remove(const FileReplica& replica) throw (DmException)
{
  hdfsDelete(this->fs, replica.rfn);
}



Location HadoopPoolHandler::putLocation(const std::string& fn) throw (DmException)
{
  // Get the path to create (where the file will be put)
  std::string path;
  path = fn.substr(0, fn.find_last_of('/'));

  // Create the path
  hdfsCreateDirectory(this->fs, path.c_str());
  
  // Uri returned_uri;
  Location loc("dpmhadoop-data1.cern.ch", fn.c_str(), true, 0);
  
  // Add this replica
  struct stat s = this->stack->getCatalog()->extendedStat(fn).stat;
  this->stack->getCatalog()->addReplica(std::string(), s.st_ino,
                                        std::string(),
                                        loc.path, '-', 'P',
                                        this->poolName, std::string());
  
  // No token used
  return loc;
}



void HadoopPoolHandler::putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException)
{
  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, replica.rfn);
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR,
                      "hdfsGetPathInfo has failed on URI %s", replica.rfn);
  size_t size = fileStat->mSize;
  hdfsFreeFileInfo(fileStat, 1);

  this->stack->getINode()->changeSize(replica.fileid, size);
}



// HadoopIOFactory implementation
HadoopFactory::HadoopFactory() throw (DmException)
{
  // Nothing
}



void HadoopFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option %s not recognised", key.c_str());
}



IODriver* HadoopFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new HadoopIODriver();
}



HadoopIODriver::HadoopIODriver()
{
  this->fs = hdfsConnectAsUser("dpmhadoop-name.cern.ch", 8020, "dpmmgr");
  if(!this->fs)
    throw DmException(DM_INTERNAL_ERROR, "Could not open the Hadoop Filesystem");
}



HadoopIODriver::~HadoopIODriver()
{
  hdfsDisconnect(this->fs);
}



void HadoopIODriver::setStackInstance(StackInstance*) throw (DmException)
{
  // Nothing
}



void HadoopIODriver::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // Nothing
}



IOHandler *HadoopIODriver::createIOHandler(const std::string& uri,
                                           int openmode,
                                           const std::map<std::string, std::string>& extras) throw (DmException)
{
	return new HadoopIOHandler(this, uri, openmode);
}



struct stat HadoopIODriver::pStat(const std::string& pfn) throw (DmException)
{ 
  if(hdfsExists(this->fs, pfn.c_str()) != 0)
    throw DmException(DM_INTERNAL_ERROR, "URI %s Does not exists", pfn.c_str());

  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, pfn.c_str());
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR,
                      "hdfsGetPathInfo has failed on URI %s", pfn.c_str());

  struct stat s;
  memset(&s, 0, sizeof(s));

  s.st_atime = fileStat->mLastAccess; 
  s.st_mtime = fileStat->mLastMod;
  s.st_size = fileStat->mSize;  
  

  hdfsFreeFileInfo(fileStat, 1);
  return s;
}



std::string HadoopFactory::implementedPool() throw ()
{
  return "hadoop";
}



PoolDriver* HadoopFactory::createPoolDriver() throw (DmException)
{
  return new HadoopPoolDriver();
}



static void registerPluginHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<PoolDriverFactory*>(new HadoopFactory()));
}



static void registerIOHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<IOFactory*>(new HadoopFactory()));
}



/// This is what the PluginManager looks for
PluginIdCard plugin_hadoop_pooldriver = {
  PLUGIN_ID_HEADER,
  registerPluginHadoop
};

PluginIdCard plugin_hadoop_io = {
  PLUGIN_ID_HEADER,
  registerIOHadoop
};
