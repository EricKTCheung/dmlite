//
//  A simple prototype for DPM REST gateway using Fastcgi 
//
//  TODO: Add signal handlers!

#include <fcgi_config.h>

#include <sys/statvfs.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/mman.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <fcgiapp.h>
#include <fcgio.h>

#include <math.h>
#include <time.h>
#include <stdio.h>
#include <queue>
#include <iostream>
#include <sstream>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/authn.h>

#include <openssl/md5.h>
#include <mysql.h>
#include <davix.hpp>

#define THREAD_POOL_SIZE 20
#define STACKINSTANCE_POOL_SIZE 20

// mysql stuff
const char* MYSQL_HOST = "localhost";
const char* MYSQL_USER = "dpmmgr";
const char* MYSQL_PASS = "MYSQLPASS";

//static int counts[THREAD_POOL_SIZE];
static pthread_mutex_t accept_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t counts_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t dmlite_mutex = PTHREAD_MUTEX_INITIALIZER;

// Fastcgi mode
const std::string FCGI_HEAD_MODE = "head";
const std::string FCGI_DISK_MODE = "disk";

// Fastcgi url prefix
const std::string FCGI_HEAD_PREFIX = "/dpm-rest";
const std::string FCGI_DISK_PREFIX = "/fcgi-disk";

// query params
const std::string PARAM_CHECKSUM = "chksum";
const std::string PARAM_FREESPACE_ALL = "freespace-all";
//const std::string PARAM_FREESPACE = "freespace"; 
const std::string PARAM_LFN = "lfn";

// not used at the moment
struct worker_args{
    std::queue<dmlite::StackInstance *> *siqueue;
};

// for storing http url queries
typedef std::map<std::string, std::string> queryMap;

// split query string into pairs and store in map
void queryToMap(queryMap& q_map, std::string query_string){
    std::string::iterator itr;
    std::string key, value;

    std::cout << std::endl << query_string << std::endl;

    for(itr = query_string.begin(); itr != query_string.end(); ){
        std::string::iterator tmp_itr = std::find(itr, query_string.end(), '&');

        if(tmp_itr == query_string.end()){ // just 1 set of query, process it and return
            tmp_itr = std::find(itr, query_string.end(), '=');

            if(tmp_itr == query_string.end()){ // no param
                key = query_string;
                value = "";
            }else{
                key = std::string(itr, tmp_itr);
                value = std::string(tmp_itr+1, query_string.end());
            }

            std::cout << std::endl << key << " " << value << std::endl;

            q_map.insert(std::pair<std::string, std::string>(key, value));
            return;
        }else{
            std::string tmp_string(itr, tmp_itr);
            std::string::iterator tmp_itr2 = std::find(tmp_string.begin(), tmp_string.end(), '=');

            if(tmp_itr2 == tmp_string.end()){ // no param
                key = tmp_string;
                value = "";
            }else{
                key = std::string(tmp_string.begin(), tmp_itr2);
                value = std::string(tmp_itr2+1, tmp_string.end());
            }
            q_map.insert(std::pair<std::string, std::string>(key, value));

            std::cout << std::endl << key << " " << value << std::endl;

            itr = tmp_itr;

            if(tmp_itr != query_string.end())
                ++itr;
        }

    }
}

void queryToVector(std::vector<std::string> query_vec, std::string query_string){
    std::string::iterator itr = std::find(query_string.begin(), query_string.end(), '=');

    std::string key(query_string.begin(), itr);
    std::string value(itr+1, query_string.end());

    query_vec.push_back(key);
    query_vec.push_back(value);
}

// Calculate md5 digest of file, not much to say here
int calculateMD5(std::string& result, int fd){
    unsigned char result_buf[MD5_DIGEST_LENGTH];

    struct stat statbuf;
    if(fstat(fd, &statbuf) < 0)
        return -1;

    void* file_buf = mmap(0, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
    MD5((unsigned char*) file_buf, statbuf.st_size, result_buf);
    munmap(file_buf, statbuf.st_size);

    result = dmlite::checksums::hexPrinter(result_buf, MD5_DIGEST_LENGTH);
    return 0;
}

// Access point to different checksum handlers
// will probably want to pass something like a DavixError* in here to record error type for the real thing
int calculateChksum(std::string phy_path, std::string& result, std::string csumtype){
    int ret = -1;

    int fd = open(phy_path.c_str(), O_RDONLY);
    if(fd < 0)
        return -1;

    std::cout << std::endl << std::endl << "Inside calculateChksum and csumtype is: " << csumtype << std::endl << std::endl;
    
    if(csumtype.compare("MD")==0){
        ret = calculateMD5(result, fd);        
    }else if(csumtype.compare("AD")==0){ // not implemeneting other type of checksum for this prototype for now
        // . . .
    }else if(csumtype.compare("CS")==0){
        // . . .
    }

    close(fd);
    return ret;
}

dmlite::StackInstance* getStackInstance(std::queue<dmlite::StackInstance *> *siqueue){
    dmlite::StackInstance* si = NULL;

    pthread_mutex_lock(&dmlite_mutex);
    if(siqueue->size() > 0){
        si = siqueue->front();
        siqueue->pop();
    }
    pthread_mutex_unlock(&dmlite_mutex);

    return si;
}

void releaseStackInstance(std::queue<dmlite::StackInstance *> *siqueue, dmlite::StackInstance* si){
    if(si){
        pthread_mutex_lock(&dmlite_mutex);
        siqueue->push(si);
        pthread_mutex_unlock(&dmlite_mutex);
    }
}

// Get and set user identity
int setUserIdent(dmlite::StackInstance* si, const FCGX_Request& req){
    dmlite::SecurityCredentials creds;
    //FIXME this does not work because I suspect the number is related to uid or something simular
    // need to find a better break condition
    int MAX_TRY = 500000;
    bool id_found = false;    

    for(int i=0; ; ++i){
        char grid_param[32];
        sprintf(grid_param, "GRST_CRED_AURI_%d", i);
        char* tmp =  FCGX_GetParam(grid_param, req.envp);
        if(tmp != NULL){
            FCGX_FPrintF(req.out, "%s<p>\r\n", tmp);
            if(i==0){
                //get rid of '+'s in string, won't match mapfile otherwise
                for(int j=0; tmp[j]!='\0'; ++j){
                    if(tmp[j] == '+')
                        tmp[j] = ' ';
                }
                //now get rid of the prefix
                creds.clientName = tmp + 3;
                creds.remoteAddress = FCGX_GetParam("REMOTE_ADDR", req.envp);
            }
            id_found = true;
        }
        else if(id_found){ // no user ident available
            std::cout << std::endl << "Got user identification: GRST_CRED_AURI_" << i << " " << creds.clientName << std::endl;
            //return 0;
            break;
        }else if(i > MAX_TRY){
            std::cout << std::endl << "Could not get user identification" << std::endl;
            return -1;
        }
    }

    try {
        si->setSecurityCredentials(creds);
    }
    catch (dmlite::DmException& e) {
        std::cout << "Could not set the credentials." << std::endl
            << "Reason: " << e.what() << std::endl;
        return -1;
    }    
    return 0;
}


// Main logic for the REST get/set checksum call, flow of execution defers depending on fcgi server's location (head/disk node)
int getChksum(std::queue<dmlite::StackInstance *> *si_queue, const FCGX_Request& req, Davix::Context& c, queryMap& qmap){
    int ret = -1;

    // see if we are running on head node or disk node
    std::string fcgi_mode = FCGX_GetParam("FCGI_MODE", req.envp);

    if(fcgi_mode.compare(FCGI_HEAD_MODE) == 0){ // head node
        std::cout << "Inside getChksum - HEAD" << std::endl;

        dmlite::StackInstance* si = getStackInstance(si_queue);

        if(si == NULL) 
            return -1;

        if(setUserIdent(si, req) < 0)
            return -1;

        // check db and see if checksum for requested file already exist
        std::string path = FCGX_GetParam("PATH_INFO", req.envp);
        if(!path.empty())
            path = "/dpm" + path;

        dmlite::Catalog* catalog = si->getCatalog();
        dmlite::ExtendedStat st = catalog->extendedStat(path, true); 
        dmlite::Extensible csumList, csum;

        //test
        std::cout << "Checksum for " << path << " : " << st.csumvalue  << std::endl;

        // if no checksum exists for requested file, send request to disk node
        if(st.csumvalue.empty()){
            // Here we need some logic to keep track of the state of previous checksum requests for the same file,
            // We don't want to flood the disk nodes with checksum request if it is already being calculated.
            // Perhaps a resource pending mechanism like UGR?

            dmlite::PoolManager* poolManager = si->getPoolManager();

            try {
                dmlite::Location location = poolManager->whereToRead(path);

                if(location.size() > 0){
                    std::string loc = location[0].url.domain + FCGI_DISK_PREFIX  + location[0].url.path;
                    std::cout << loc << std::endl;

                    // construct request
                    Davix::DavixError* dav_err;
                    Davix::Uri uri("http://" + location[0].url.domain + 
                            FCGI_DISK_PREFIX  + 
                            location[0].url.path + 
                            "?" + PARAM_CHECKSUM + "=" + 
                            ((qmap.find(PARAM_CHECKSUM)->second.empty()) ? "md5" : qmap.find(PARAM_CHECKSUM)->second) + 
                            "&" + PARAM_LFN  + "=" + 
                            path);

                    std::cout << std::endl << uri.getString() << std::endl;

                    Davix::GetRequest r(c, uri, &dav_err);                    
                    r.executeRequest(&dav_err);
                    // add checks for dav_err

                    const char* buf = r.getAnswerContent();
                    int code = r.getRequestCode();

                    // need to decide what to do with different status codes in the real thing
                    std::cout << std::endl << std::endl << "Code: " << code << std::endl << std::endl;

                    // parse request body and set the requested checksum in db

                    // send result back to client
                    FCGX_FPrintF(req.out, 
                            "Content-type: text/html\r\n"
                            "\r\n"
                            "%s\r\n", buf);
                }
            }
            catch (dmlite::DmException& e) {
                std::cout << "Could not get the final location for "
                    << path << std::endl
                    << "Reason: " << e.what() << std::endl;
                return e.code();
            }

        }else{ // requested checksum found in db, return it to client
            csum["csumtype"] = st.csumtype;
            csum["csumvalue"] = st.csumvalue;
            csumList[path] = csum;
            FCGX_FPrintF(req.out, "Content-type: application/json\r\n"
                    "\r\n"
                    "%s\r\n", csumList.serialize().c_str());
        }

        // release stackinstance back to the queue for next request
        releaseStackInstance(si_queue, si);

    }else if(fcgi_mode.compare(FCGI_DISK_MODE) == 0){ // disk node
        // receive request from head node
        std::cout << std::endl << "Inside getChksum in DISK NODE!" << std::endl;

        dmlite::StackInstance *si;
        pthread_mutex_lock(&dmlite_mutex);
        si = si_queue->front();
        si_queue->pop();
        pthread_mutex_unlock(&dmlite_mutex);

        dmlite::Catalog* catalog = si->getCatalog();

        std::string path = FCGX_GetParam("PATH_INFO", req.envp);
        if(path.empty()){ // warn and bail
            std::cout << std::endl << path << std::endl;
            return -1;
        }

        std::string lfn(qmap.find(PARAM_LFN)->second);
        std::cout << std::endl << "lfn: " << lfn << std::endl;

        // actually calculate the checksum here
        std::string csumtype(qmap.find(PARAM_CHECKSUM)->second);
        std::string csumvalue;

        // might as well make the names of checksum types compatible to those in dmlite
        
        if(!dmlite::checksums::isChecksumFullName(csumtype))
            csumtype = "checksum." + csumtype;
        
        csumtype = dmlite::checksums::shortChecksumName(csumtype);

        // the whole csumtype check could probably be done in the head node instead
        if(csumtype.empty()){ // incompatable checksum type, send client warning and bail
            return -1;
        }

        std::string response_body, content_type;
        int ret = calculateChksum(path, csumvalue, csumtype);
        int status_code = 0;
        dmlite::Extensible csumList, csum;

        if(ret < 0){ // op failed, what to send back to head node?
            // probably will want handler for more status code in the real thing, but skipping other codes in prototype
            status_code = 500;
            content_type = "text/html";
            response_body = "Failed to calculate checksum for " + lfn;
        }else{
            status_code = 201;
            csum["csumtype"] = csumtype;
            csum["csumvalue"] = csumvalue;
            csumList[lfn] = csum;
            content_type = "application/json";
            response_body = csumList.serialize();
        }

        catalog->setChecksum(lfn, csumtype, csumvalue);

        // from here onwards we have to decide what to send back to the fcgi server in head node
        FCGX_FPrintF(req.out,
                "Status: %d\r\n"
                "Content-type: %s\r\n"
                "\r\n"
                "%s\r\n", status_code, content_type.c_str(), response_body.c_str());

        releaseStackInstance(si_queue, si);

    }else{ // something is really wrong if we got here, bailout

    }
}

// get free space from disk nodes
int getFreeSpaceAll(std::queue<dmlite::StackInstance *> *si_queue, const FCGX_Request& req, Davix::Context& c, queryMap& qmap){
    // see if we are running on head node or disk node
    std::string fcgi_mode = FCGX_GetParam("FCGI_MODE", req.envp);

    if(fcgi_mode == FCGI_HEAD_MODE){ // head node
        std::cout << "Inside getFreeSpaceAll - HEAD node" << std::endl;

        // don't need dmlite here really, if we just want to talk to the db and get stuff from dmp_fs table

        // creating a new connection to db per query would defeats the main advantage of using fastcgi,
        // where the expensive init routines can be done at startup (db connection pool etc)
        // but for this prototype this will do for now

        MYSQL *con = mysql_init(NULL);

        if(mysql_real_connect(con, MYSQL_HOST, MYSQL_USER, MYSQL_PASS, "dpm_db", 0, NULL, 0) == NULL){
            mysql_close(con);
            return -1;
        }

        if(mysql_query(con, "SELECT server,fs FROM dpm_fs")){
            mysql_close(con);
            return -1;
        }

        MYSQL_RES *result = mysql_store_result(con);
        if(result == NULL){
            mysql_close(con);
            return -1;
        }

        int num_fields = mysql_num_fields(result);

        MYSQL_ROW row;
        std::cout << std::endl;

        std::stringstream ss;
        std::vector<std::string> fs_vec;
        std::vector<std::string> key_vec; // used as unique keys for fs space status map later

        int y = 0;
        while((row = mysql_fetch_row(result))){
            if(!row[0] || !row[1])
                continue;

            fs_vec.push_back(row[0] + FCGI_DISK_PREFIX + row[1]);
            key_vec.push_back(std::string(row[0]) + std::string(row[1]));
            std::cout << std::endl << fs_vec[y] << std::endl;
            ++y;
        }

        // now for every fs we get from the query, we send a request to the disk server hosting it
        // but we need to send them in parallel, how?

        // perhaps better to make a class that can hold all the info of each request (DavixError, server name, fs, fs free space etc)

        // can use davix task queue here later to add concurrency  

        std::vector<Davix::GetRequest*> requestVec;
        Davix::DavixError* dav_err = NULL;
        std::string uri;

        // TODO: this infomation should be kept in memory throughout the life time of the process
        dmlite::Extensible spaceStatusMap;

        for(int i=0; i < fs_vec.size(); ++i){
            uri = "http://" + fs_vec[i] + "?" + PARAM_FREESPACE_ALL;
            std::cout << std::endl << uri << std::endl;

            //requestVec.push_back(new Davix::GetRequest(c, Davix::Uri(uri), &dav_err));
            //requestVec[i]->executeRequest(&dav_err); 

            Davix::GetRequest r(c, Davix::Uri(uri), &dav_err);
            r.executeRequest(&dav_err);

            if(!dav_err){
                const char* buf = r.getAnswerContent();
                int code = r.getRequestCode();
                std::cout << std::endl << "Answer: " << buf << std::endl;
                std::cout << std::endl << "Code: " << code << std::endl;

                dmlite::Extensible fs_space_status;

                try{
                    fs_space_status.deserialize(std::string(buf));

                    //std::cout << std::endl << "Response from " << fs_vec[i] << " " << dmlite::Extensible::anyToU64(fs_space_status["free_space"]) << "/" << dmlite::Extensible::anyToU64(fs_space_status["total_space"]) << std::endl;
                    std::cout << std::endl << "Response from " << key_vec[i] << " " << fs_space_status.getU64("free_space") << "/" << fs_space_status.getU64("total_space") << std::endl;
                    spaceStatusMap[key_vec[i]] = fs_space_status;
                }
                catch (dmlite::DmException& e) {
                    std::cout << "Could not deserialise data for "
                        << fs_vec[i] << std::endl
                        << "Reason: " << e.what() << std::endl;
                    return e.code();
                }

            }else{ // request has failed
                // record and skip to the next request?
                Davix::DavixError::clearError(&dav_err);
            }

        }

        // assuming everything went ok, spaceStatusMap should contain space info of all the fs requested by client
        // now send it back
        FCGX_FPrintF(req.out,
                //"Status: %d\r\n"
                "Content-type: application/json\r\n"
                "\r\n"
                "%s\r\n", spaceStatusMap.serialize().c_str());


        mysql_free_result(result);
        mysql_close(con);

        return 0;

    }else if(fcgi_mode == FCGI_DISK_MODE){ // disk node
        std::cout << std::endl << "Inside getFreeSpaceAll - DISK node" << std::endl;

        // if we got here we should be inside the disk node which space status has been requested
        // just calculate it and return the info to head node
        std::string path = FCGX_GetParam("PATH_INFO", req.envp);
        std::cout << std::endl << "Path to stat: " << path << std::endl;

        struct statvfs buf;

        if(statvfs(path.c_str(), &buf) != 0){
            std::cout << std::endl << "Error in statvfs" << std::endl;
            return -1;
        }

        // for root
        //uint64_t free_space = static_cast<uint64_t>(buf.f_bfree) * static_cast<uint64_t>(buf.f_frsize);

        uint64_t total_space = static_cast<uint64_t>(buf.f_blocks) * static_cast<uint64_t>(buf.f_frsize);
        // for non-root
        uint64_t free_space_unpriv = static_cast<uint64_t>(buf.f_bavail) * static_cast<uint64_t>(buf.f_frsize);

        std::cout << std::endl << "Space reporting: " << free_space_unpriv << "/" << total_space << std::endl;

        dmlite::Extensible data;

        // problem, can't send numbers bigger than 32bit int through json
        // send the data as stings I guess, just have to remember to convert them back to uint_64 for calculations

        data["free_space"] = dmlite::Extensible::anyToString(free_space_unpriv);
        data["total_space"] = dmlite::Extensible::anyToString(total_space);

        // these will fail when try to deserialize
        //data["free_space"] = 2147483648;
        //data["total_space"] = 2147483648;

        std::cout << std::endl << data.serialize() << std::endl;

        // send data back to head node
        FCGX_FPrintF(req.out,
                //"Status: %d\r\n"
                "Content-type: application/json\r\n"
                "\r\n"
                "%s\r\n", data.serialize().c_str());

        return 0;

    }else{ // something has gone really wrong

    }
}

// entry point for worker threads, endless loop that wait for requests from apache
// pass on processing to handlers depends on (not yet) defined REST methods
static void *acceptLoop(void *args)
{
    //int rc, i, thread_id = (long)a;
    int rc, i = 0;
    //worker_args* args = (worker_args*)a;
    std::queue<dmlite::StackInstance *> *siqueue = (std::queue<dmlite::StackInstance *>*)args;
    //pid_t pid = getpid();
    FCGX_Request request;
    //char *server_name;

    FCGX_InitRequest(&request, 0, 0);

    Davix::Context davix_context;

    while(true)
    {
        // thread safety seems to be platform dependant... serialise just in case
        pthread_mutex_lock(&accept_mutex);
        rc = FCGX_Accept_r(&request);
        pthread_mutex_unlock(&accept_mutex);

        if (rc < 0)
            break;

        std::string request_method = FCGX_GetParam("REQUEST_METHOD", request.envp);
        std::string query = FCGX_GetParam("QUERY_STRING", request.envp);

        // split query string first before comparing
        queryMap query_map;
        queryToMap(query_map, query);       

        if(request_method == "GET"){
            // there may be multiple calls using the GET verb
            int ret = -1;

            // a better way might be to use a map with different ops paired with pointer to their handlerd
            if(query.empty()){ // REST GET with no query string, functions to be decided
                // ...
                // get file or directory listing perhaps
            }
            // get checksum of file
            else if(query_map.find(PARAM_CHECKSUM) != query_map.end()){
                ret = getChksum(siqueue, request, davix_context, query_map);
            }
            // list all free space
            else if(query_map.find(PARAM_FREESPACE_ALL) != query_map.end()){
                ret = getFreeSpaceAll(siqueue, request, davix_context, query_map);
            }
            // list free space of pool

        }else if(request_method == "HEAD"){ // meaningless placeholder
            FCGX_FPrintF(request.out, 
                    "Content-type: text/html\r\n"
                    "\r\n"
                    "This is a HEAD\r\n");

        }else if(request_method == "PUT"){ // meaningless placeholder
            FCGX_FPrintF(request.out, 
                    "Content-type: text/html\r\n"
                    "\r\n"
                    "This is a PUT\r\n");
        }
        /*
           dmlite::Catalog* catalog = si->getCatalog();

           std::string dir_to_open = "/dpm/cern.ch/home/dteam";

           try {
           dmlite::Directory* dir = catalog->openDir(dir_to_open);
           dmlite::ExtendedStat* xstat;

           std::cout << "Content of the directory " << dir_to_open << std::endl;
           while ((xstat = catalog->readDirx(dir)) != NULL) {
        //std::cout << '\t' << xstat->name << std::endl;
        FCGX_FPrintF(request.out, "%s\r\n", xstat->name.c_str());
        }

        catalog->closeDir(dir);
        }
        catch (dmlite::DmException& e) {
        std::cout << "Could not open the directory." << std::endl
        << "Reason: " << e.what() << std::endl;
        return (void*)e.code();
        }

        if(!path.empty())
        path = "/dpm" + path;

        dmlite::PoolManager* poolManager = si->getPoolManager();

        try {
        dmlite::Location location = poolManager->whereToRead(path);
        dmlite::Location::const_iterator iter;
        int n;

        for (iter = location.begin(), n = 0; iter != location.end(); ++iter, ++n) {
        std::cout << "Chunk " << n << ": "
        //<< iter->host << ':' << iter->url.path << " ("
        << "Domain: " << iter->url.domain  << " Path: " << iter->url.path << " ("

        << iter->offset << '-' << iter->offset + iter->size << ")"
        << std::endl;

        std::vector<std::string> keys = iter->getKeys();
        std::vector<std::string>::const_iterator j;

        for (j = keys.begin(); j != keys.end(); ++j) {
        std::cout << '\t' << *j << ": " << iter->getString(*j) << std::endl;
        }

        }
        }
        catch (dmlite::DmException& e) {
        std::cout << "Could not get the final location for "
        << path << std::endl
        << "Reason: " << e.what() << std::endl;
        return (void*)e.code();
        }
        */
        FCGX_Finish_r(&request);
    }

    return NULL;
}

int main(void)
{
    // instansiate resources and configure them before going into accept loops

    // probably want to init a db connection pool here if not using dmlite

    // instansiate dmlite
    dmlite::PluginManager manager;

    try {
        manager.loadConfiguration("/etc/dmlite.conf");
    }
    catch (dmlite::DmException& e) {
        std::cout << "Could not load the configuration file." << std::endl
            << "Reason: " << e.what() << std::endl;
        return 1;
    }

    std::queue<dmlite::StackInstance *> siqueue;

    // Instansiate a pool of dmlite stack instaces 
    for(int i=0; i < STACKINSTANCE_POOL_SIZE; ++i){
        // Create StackInstance
        dmlite::StackInstance *si = 0;
        si = new dmlite::StackInstance(&manager);
        if(si)  
            siqueue.push(si);
    }

    int i;
    pthread_t id[THREAD_POOL_SIZE];

    // init must be called for multithreaded applications
    FCGX_Init();

    for (i = 1; i < THREAD_POOL_SIZE; ++i)
        pthread_create(&id[i], NULL, acceptLoop, (void*)&siqueue);

    // use main thread as worker as well
    acceptLoop((void*)&siqueue);

    return 0;
}

