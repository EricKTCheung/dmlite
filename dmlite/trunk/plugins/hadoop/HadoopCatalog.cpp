#include "HadoopCatalog.h"

namespace dmlite{

HadoopCatalog::HadoopCatalog(Catalog *decorates, std::string namenode, std::string port, std::string user) throw (DmException): DummyCatalog(decorates){
	this->fs =  hdfsConnectAsUser(namenode.c_str(), atoi(port.c_str()), user.c_str());
}

HadoopCatalog::~HadoopCatalog() throw (DmException){
	hdfsDisconnect(this->fs);
}

FileReplica HadoopCatalog::get (const std::string& file) throw (DmException){

 	hdfsGetDatanode(NULL);	

/*        if(hdfsExists(this->fs, file.c_str()) == 0){
                char*** hosts = hdfsGetHosts(this->fs, file.c_str(), 0, 1);
                printf("Host list owning the first block:\n");
                if(hosts){
                        int i=0;
                        while(hosts[i]) {
                                int j = 0;
                                while(hosts[i][j]) {
                                        printf("%s\n", hosts[i][j]);
                                        ++j;
                                }
                                ++i;
                        }
                        hdfsFreeHosts(hosts);
                }
        }*/

}
};

