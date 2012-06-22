/// @file    plugins/hadoop/Hadoop.h
/// @brief   hadoop plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include "Hadoop.h"

using namespace dmlite;

HadoopIOHandler::HadoopIOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException):
  path_(uri)
{

  // Set up correctly the flag to open a hdfs file
  int flag;
  if(openmode == std::ios_base::in)
    flag = O_RDONLY;
  else if (openmode == std::ios_base::out)
    flag = O_WRONLY;
  else
    throw DmException(DM_INTERNAL_ERROR, "Could not understand the openmode");

  // Try to connect the Hdfs, trigger an exception otherwise
  this->fs = hdfsConnectAsUser("dpmhadoop-name.cern.ch", 8020, "dpmmgr");
  if(!this->fs)
    throw DmException(DM_INTERNAL_ERROR, "Could not open the Hadoop Filesystem");

  // Try to open the hdfs file, map the errno to the DmException otherwise
  this->file = hdfsOpenFile(this->fs, uri.c_str(), flag, 0, 0, 0);
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
    hdfsCloseFile(this->fs, this->file);
  this->file = 0;

  // Disconnect from the Hadoop FS
  if(this->fs)
    hdfsDisconnect(this->fs);
  this->fs = 0;
}

void HadoopIOHandler::close(void) throw (DmException)
{
 // Close the file if its open
  if(this->file)
    hdfsCloseFile(this->fs, this->file);
  this->file = 0;
}

size_t HadoopIOHandler::read(char* buffer, size_t count) throw (DmException){
	size_t bytes_read = hdfsRead(this->fs, this->file, buffer, count);
 
        /* EOF flag is returned if the number of bytes read is lesser than the requested */
	if (bytes_read < count)
		this->isEof = true;

	return bytes_read;
}

/* Write a chunk of a file in a Hadoop FS*/
size_t HadoopIOHandler::write(const char* buffer, size_t count) throw (DmException){
	return hdfsWrite(this->fs, this->file, buffer, count);
}

/* Position the reader pointer to the desired offset */
void HadoopIOHandler::seek(long offset, std::ios_base::seekdir whence) throw (DmException){

	long positionToSet = 0;	

	/* Whence described from where the offset has to be set (begin, current or end) */
	switch(whence)
	{
		case std::ios_base::beg:
			positionToSet = offset;
			break;	
		case std::ios_base::cur:
			positionToSet = (this->tell() + offset);
			break;
		case std::ios_base::end:
			positionToSet = (hdfsAvailable(this->fs, this->file) - offset);
			break;
		default:
                  break;
	}

	hdfsSeek(this->fs, this->file, positionToSet);
}

long HadoopIOHandler::tell(void) throw (DmException){
	return hdfsTell(this->fs, this->file);
}

void HadoopIOHandler::flush(void) throw (DmException){
	hdfsFlush(this->fs, this->file);
}

bool HadoopIOHandler::eof(void) throw (DmException){
	return this->isEof;
}

struct stat HadoopIOHandler::pstat(void) throw (DmException) {
  if(hdfsExists(this->fs, this->path_.c_str()) != 0)
    throw DmException(DM_INTERNAL_ERROR, "URI %s Does not exists", this->path_.c_str());

  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, this->path_.c_str());
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR,
                      "hdfsGetPathInfo has failed on URI %s", this->path_.c_str());

  struct stat s;
  memset(&s, 0, sizeof(s));

  s.st_atime = fileStat->mLastAccess; 
  s.st_mtime = fileStat->mLastMod;
  s.st_size = fileStat->mSize;  
  

  hdfsFreeFileInfo(fileStat, 1);
  return s;
}


/* Hadoop pool handling */
HadoopPoolDriver::HadoopPoolDriver(StackInstance* si, const Pool& pool) throw (DmException):
            stack(si), pool(pool)
{
  PoolMetadata* meta = this->stack->getPoolManager()->getPoolMetadata(pool);
  
  this->host  = meta->getString("hostname");
  this->uname = meta->getString("username");
  this->port  = meta->getInt("port");
  
  this->fs = hdfsConnectAsUser(this->host.c_str(),
                               this->port,
                               this->uname.c_str());
  delete meta;
  
  if (this->fs == 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not instantiate the HadoopPoolDriver");
}

HadoopPoolDriver::~HadoopPoolDriver()
{
  if(this->fs)
    hdfsDisconnect(this->fs);
}

void HadoopPoolDriver::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // TODO
}

std::string HadoopPoolDriver::getPoolType(void) throw (DmException)
{
  return this->pool.pool_type;
}

std::string HadoopPoolDriver::getPoolName(void) throw (DmException)
{
  return this->pool.pool_name;
}

uint64_t HadoopPoolDriver::getTotalSpace(void) throw (DmException)
{
  tOffset total = hdfsGetCapacity(this->fs);
  if (total < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the total capacity of %s", this->pool.pool_name);
  return total;
}

uint64_t HadoopPoolDriver::getUsedSpace(void) throw (DmException)
{
  tOffset used = hdfsGetUsed(this->fs);
  if (used < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the free space of %s", this->pool.pool_name);

  return used;
}

uint64_t HadoopPoolDriver::getFreeSpace(void) throw (DmException)
{
  return this->getTotalSpace() - this->getUsedSpace();
}

bool HadoopPoolDriver::isAvailable(bool write) throw (DmException)
{
  // TODO
  return true;
}

bool HadoopPoolDriver::replicaAvailable(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  if(hdfsExists(this->fs, sfn.c_str()) == 0)
    return true;
  return false;
}

Location HadoopPoolDriver::getLocation(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  // To be done
//  throw DmException(DM_NOT_IMPLEMENTED, "hadoop::getLocation");
  std::vector<std::string> datanodes;
  if(hdfsExists(this->fs, sfn.c_str()) == 0){
    char*** hosts = hdfsGetHosts(this->fs, sfn.c_str(), 0, 1);
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
    throw DmException(DM_NO_REPLICAS, "No replicas found on Hadoop for " + sfn);
  }

  // Pick a location
  srand(time(NULL));
  unsigned i = rand() % datanodes.size();
  return Location(datanodes[i].c_str(), sfn.c_str(),
                  this->replicaAvailable(sfn, replica),
                  0);
}

void HadoopPoolDriver::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  hdfsDelete(this->fs, sfn.c_str());
}

Location HadoopPoolDriver::putLocation(const std::string& fn) throw (DmException)
{
  /* Get the path to create (where the file will be put) */
  std::string path;
  path = fn.substr(0, fn.find_last_of('/'));

  /* Create the path */
  hdfsCreateDirectory(this->fs, path.c_str());
  
  //Uri returned_uri;
  Location loc("dpmhadoop-data1.cern.ch", fn.c_str(), true, 0);
  
  // Add this replica
  struct stat s = this->stack->getCatalog()->stat(fn);
  this->stack->getCatalog()->addReplica(std::string(), s.st_ino,
                                        std::string(),
                                        loc.path, '-', 'P',
                                        this->pool.pool_name, std::string());
  
  // No token used
  return loc;
}

void HadoopPoolDriver::putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException)
{
  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, replica.rfn);
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR,
                      "hdfsGetPathInfo has failed on URI %s", replica.rfn);
  size_t size = fileStat->mSize;
  hdfsFreeFileInfo(fileStat, 1);

  this->stack->getINode()->changeSize(replica.fileid, size);
}

/* HadoopIOFactory implementation */
HadoopIOFactory::HadoopIOFactory() throw (DmException)
{
  // Nothing
}

void HadoopIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option %s not recognised", key.c_str());
}

IOHandler *HadoopIOFactory::createIO(const StackInstance* si,
                                     const std::string& uri, std::iostream::openmode openmode,
                                     const std::map<std::string, std::string>& extras) throw (DmException)
{
	return new HadoopIOHandler(uri, openmode);
}

struct stat HadoopIOFactory::pStat(const StackInstance* si, const std::string& pfn) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "pStat not implemented");
  /*
  if(hdfsExists(this->fs, pfn.c_str()) != 0)
    throw DmException(DM_INTERNAL_ERROR, "URI %s Does not exists", pfn.c_str());

  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, this->path_.c_str());
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR,
                      "hdfsGetPathInfo has failed on URI %s", this->path_.c_str());

  struct stat s;
  memset(&s, 0, sizeof(s));

  s.st_atime = fileStat->mLastAccess; 
  s.st_mtime = fileStat->mLastMod;
  s.st_size = fileStat->mSize;  
  

  hdfsFreeFileInfo(fileStat, 1);
  return s;*/
}

std::string HadoopIOFactory::implementedPool() throw ()
{
  return "hadoop";
}

PoolDriver* HadoopIOFactory::createPoolDriver(StackInstance* si, const Pool& pool) throw (DmException)
{
  if (this->implementedPool() != std::string(pool.pool_type))
    throw DmException(DM_UNKNOWN_POOL_TYPE, "Hadoop does not recognise the pool type %s", pool.pool_type);
  return new HadoopPoolDriver(si, pool);
}

static void registerPluginHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<PoolDriverFactory*>(new HadoopIOFactory()));
}

static void registerIOHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<IOFactory*>(new HadoopIOFactory()));
}

/// This is what the PluginManager looks for
PluginIdCard plugin_hadoop_pooldriver = {
  API_VERSION,
  registerPluginHadoop
};

PluginIdCard plugin_hadoop_io = {
  API_VERSION,
  registerIOHadoop
};
