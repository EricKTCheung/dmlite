/// @file    plugins/hadoop/Hadoop.h
/// @brief   hadoop plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include "Hadoop.h"

using namespace dmlite;

/* HadoopIOHandler implementation */
HadoopIOHandler::HadoopIOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
  this->fs = hdfsConnectAsUser("dpmhadoop-name.cern.ch", 8020, "dpmmgr");
  if(!this->fs)
    throw DmException(DM_INTERNAL_ERROR, "Could not open the Hadoop Filesystem");

  this->file = hdfsOpenFile(this->fs, uri.c_str(), O_WRONLY, 0, 0, 0);
  if(!this->file)
    throw DmException(DM_INTERNAL_ERROR, "Could not open the file %s (errno %d)", uri.c_str(), errno);

  this->isEof = false;
}

/* Hadoop IO Handler Destructor */
HadoopIOHandler::~HadoopIOHandler()
{
  /* Close the file if its still open*/
  if(this->file)
    hdfsCloseFile(this->fs, this->file);

  /* Disconnect from the Hadoop FS */
  if(this->fs)
    hdfsDisconnect(this->fs);
}

/* Method to close a file */
void HadoopIOHandler::close(void) throw (DmException)
{
 /* Close the file if its open*/
  if(this->file)
    hdfsCloseFile(this->fs, this->file);
}

/* Read a chunk of a file from a Hadoop FS */
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

/* Cleaning function (not used due to a  bad implementation)*/
void HadoopIOHandler::deleteFile(const char *filename) throw (DmException){
	hdfsDelete(this->fs, filename);
}

/* Hadoop pool handling */
HadoopPoolHandler::HadoopPoolHandler(PoolManager* pm, Pool* pool):
            manager(pm), pool(pool)
{
  PoolMetadata* meta = this->manager->getPoolMetadata(*pool);
  this->fs = hdfsConnectAsUser(meta->getString("hostname").c_str(),
                               meta->getInt("port"),
                               meta->getString("username").c_str());
  delete meta;
  
  if (this->fs == 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not instantiate the HadoopPoolHandler");
}

HadoopPoolHandler::~HadoopPoolHandler()
{
  if(this->fs)
    hdfsDisconnect(this->fs);
}

void HadoopPoolHandler::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // TODO
}

std::string HadoopPoolHandler::getPoolType(void) throw (DmException)
{
  return this->pool->pool_type;
}

std::string HadoopPoolHandler::getPoolName(void) throw (DmException)
{
  return this->pool->pool_name;
}

uint64_t HadoopPoolHandler::getTotalSpace(void) throw (DmException)
{
  tOffset total = hdfsGetCapacity(this->fs);
  if (total < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the total capacity of %s", this->pool->pool_name);
  return total;
}

uint64_t HadoopPoolHandler::getUsedSpace(void) throw (DmException)
{
  tOffset used = hdfsGetUsed(this->fs);
  if (used < 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not get the free space of %s", this->pool->pool_name);

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

bool HadoopPoolHandler::replicaAvailable(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  if(hdfsExists(this->fs, sfn.c_str()) == 0)
    return true;
  return false;
}

Uri HadoopPoolHandler::getLocation(const std::string& sfn, const FileReplica& replica) throw (DmException)
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

  srand(time(NULL));
  unsigned i = rand() % datanodes.size();
//  std::cout << datanodes[i];
  Uri returned_uri;
  strcpy(returned_uri.host, datanodes[i].c_str());
  strcpy(returned_uri.path, sfn.c_str());
  return returned_uri;
}

void HadoopPoolHandler::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  hdfsDelete(this->fs, sfn.c_str());
}

std::string HadoopPoolHandler::putLocation(const std::string& sfn, Uri* uri) throw (DmException)
{
  /* Get the path to create (where the file will be put) */
  std::string path;
  path = sfn.substr(0, sfn.find_last_of('/'));

  /* Create the path */
  hdfsCreateDirectory(this->fs, path.c_str());
  
  //Uri returned_uri;
  strcpy(uri->host, "dpmhadoop-data1");
  strcpy(uri->path, sfn.c_str());
  return std::string();

  // To be done
  // throw DmException(DM_NOT_IMPLEMENTED, "hadoop::putLocation");
}

void HadoopPoolHandler::putDone(const std::string& sfn, const Uri& pfn, const std::string& token) throw (DmException)
{
  // To be done
  throw DmException(DM_NOT_IMPLEMENTED, "hadoop::putDone");
}

/* HadoopIOFactory implementation */
HadoopIOFactory::HadoopIOFactory() throw (DmException)
{
  this->fs = hdfsConnectAsUser("dpmhadoop-name.cern.ch", 8020, "dpmmgr");
  if(!this->fs)
    throw DmException(DM_INTERNAL_ERROR, "Could not open the Hadoop Filesystem");

}

void HadoopIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
	//	std::cout << key << "  =  " << value << std::endl;
}

IOHandler *HadoopIOFactory::createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
	return new HadoopIOHandler(uri, openmode);
}

std::string HadoopIOFactory::implementedPool() throw ()
{
  return "hadoop";
}

PoolHandler* HadoopIOFactory::createPoolHandler(PoolManager* pm, Pool* pool) throw (DmException)
{
  if (this->implementedPool() != std::string(pool->pool_type))
    throw DmException(DM_UNKNOWN_POOL_TYPE, "Hadoop does not recognise the pool type %s", pool->pool_type);
  return new HadoopPoolHandler(pm, pool);
}

struct stat HadoopIOFactory::pstat(const std::string& uri) throw (DmException)
{
  if(hdfsExists(this->fs, uri.c_str()) != 0)
    throw DmException(DM_INTERNAL_ERROR, "URI %s Does not exists", uri.c_str());

  hdfsFileInfo *fileStat = hdfsGetPathInfo(this->fs, uri.c_str());
  if(!fileStat)
    throw DmException(DM_INTERNAL_ERROR, "hdfsGetPathInfo has failed on URI %s", uri.c_str());

  struct stat s;
  memset(&s, 0, sizeof(s));

  s.st_atime = fileStat->mLastAccess; 
  s.st_mtime = fileStat->mLastMod;
  s.st_size = fileStat->mSize;  
  

  hdfsFreeFileInfo(fileStat, 1);
  return s;
}

static void registerPluginHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerPoolHandlerFactory(new HadoopIOFactory());
}

static void registerIOHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerIOFactory(new HadoopIOFactory());
}

/// This is what the PluginManager looks for
PluginIdCard plugin_hadoop = {
  API_VERSION,
  registerPluginHadoop
};

PluginIdCard plugin_hadoop_io = {
  API_VERSION,
  registerIOHadoop
};
