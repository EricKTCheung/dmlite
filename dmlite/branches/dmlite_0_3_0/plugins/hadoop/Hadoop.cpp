/// @file    plugins/hadoop/Hadoop.h
/// @brief   hadoop plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include "Hadoop.h"

using namespace dmlite;

/* HadoopIOHandler implementation */
HadoopIOHandler::HadoopIOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
  this->fs = hdfsConnect("itgt-nagios-sl6", 9000);
  this->file = hdfsOpenFile(this->fs, uri.c_str(), O_RDONLY, 0, 0, 1024);
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

bool HadoopPoolHandler::replicaAvailable(const std::string&, const FileReplica& replica) throw (DmException)
{
  // No idea about this
  return true;
}

Uri HadoopPoolHandler::getPhysicalLocation(const std::string&, const FileReplica& replica) throw (DmException)
{
  // To be done
  return Uri();
}

void HadoopPoolHandler::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  hdfsDelete(this->fs, sfn.c_str());
}

/* HadoopIOFactory implementation */
HadoopIOFactory::HadoopIOFactory() throw (DmException)
{
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

static void registerPluginHadoop(PluginManager* pm) throw (DmException)
{
  pm->registerIOFactory(new HadoopIOFactory());
  pm->registerPoolHandlerFactory(new HadoopIOFactory());
}


/// This is what the PluginManager looks for
PluginIdCard plugin_hadoop = {
  API_VERSION,
  registerPluginHadoop
};
