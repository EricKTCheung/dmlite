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



/* HadoopIOFactory implementation */
HadoopIOFactory::HadoopIOFactory() throw (DmException)
{
}
void HadoopIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
	//	std::cout << key << "  =  " << value << std::endl;
}

HadoopIOHandler *HadoopIOFactory::createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
	return new HadoopIOHandler(uri, openmode);
}
