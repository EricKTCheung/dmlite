#include <dmlite/dmlite++.h>
#include <dmlite/dm_exceptions.h>
#include <hadoop/HadoopIO.h>
#include <hadoop/HadoopCatalog.h>
int main(int argn, char **argv)
{
	std::string FILE("/dpm/cern.ch/home/dteam/hadoop/log.2.gz");

	dmlite::PluginManager	*pluginManager;
	dmlite::Catalog		*catalog;

	pluginManager = new dmlite::PluginManager();
	pluginManager->loadConfiguration("/root/hadoop.conf");

	catalog = pluginManager->getCatalogFactory()->createCatalog();


	catalog->get("/dpm/log.2.gz");

//	dmlite::HadoopIOFactory io;

//	dmlite::HadoopIOHandler *handler = io.createIO("/dpm/myfile.txt", std::ios_base::in);

	//std::string test;
//	const char *test = "This file has been written by DMLITE, this is the first of a long long long serie\0";
//	char test[1024] = {'\0'};

/*	memset(test, '\0', 1024);;
	handler->read(test, 10);
	std::cout << test;


	handler->seek(10, std::ios_base::cur);

	int i = 0;	
	while(handler->eof() != true){
		memset(test, '\0', 1024);;
		handler->read(test, 3);
		std::cout << test;
		i++;
	}

 	std::cout << std::endl;
	std::cout << i << std::endl;
	delete(handler);
*/
  return 0;
}