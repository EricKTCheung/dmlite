/// @file   DomeAdapterUtils.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_UTILS_H
#define DOME_ADAPTER_UTILS_H


inline dmlite::Pool deserializePool(boost::property_tree::ptree::const_iterator it) {
    using namespace dmlite;
    using namespace boost::property_tree;

    Pool p;
    p.name = it->first;
    p.type = "filesystem";
    p["freespace"] = it->second.get<uint64_t>("freespace", 0);
    p["physicalsize"] = it->second.get<uint64_t>("physicalsize", 0);
    
    p["poolstatus"] = it->second.get<std::string>("poolstatus", "");
    p["spacetype"] = it->second.get<std::string>("spacetype", "");
    p["defsize"] = it->second.get<uint64_t>("defsize", 0);
    
    

    // fetch info about the filesystems
    std::vector<boost::any> filesystems;

    ptree fsinfo = it->second.get_child("fsinfo");

    // iterating over servers
    for(ptree::const_iterator it2 = fsinfo.begin(); it2 != fsinfo.end(); it2++) {
      // iterating over filesystems
      for(ptree::const_iterator it3 = it2->second.begin(); it3 != it2->second.end(); it3++) {
        Extensible fs;
        fs["server"] = it2->first;
        fs["fs"] = it3->first;
        fs["status"] = it3->second.get<uint64_t>("fsstatus", 0);
        fs["freespace"] = it3->second.get<uint64_t>("freespace", 0);
        fs["physicalsize"] = it3->second.get<uint64_t>("physicalsize", 0);

        filesystems.push_back(fs);
      }
    }

    p["filesystems"] = filesystems;
    return p;
}

#endif