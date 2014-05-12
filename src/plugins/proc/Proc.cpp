/// @file    plugins/proc/Proc.cpp
/// @brief   This plugin can be used to enable metadata as files
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/urls.h>
#include "IODriver.h"
#include "Proc.h"
#include "BuiltInProviders.h"

using namespace dmlite;


struct ProcDir: public Directory {
  ProcProvider* provider;
  std::list<ProcProvider*>::const_iterator i;
  ExtendedStat xs;
  struct dirent de;
};


ProcFactory::ProcFactory()
{
}


ProcFactory::~ProcFactory()
{
}


void ProcFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
}


Catalog* ProcFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  return new ProcCatalog();
}


PoolManager* ProcFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  return new ProcPoolManager();
}


IODriver* ProcFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new ProcIODriver();
}


ProcCatalog::ProcCatalog(): root_(new ProcProvider("/"))
{
  // Register some built-in providers
  ProcProvider* proc = new ProcProvider("proc");
  proc->children.push_back(new StackInfoProvider("stack"));
  proc->children.push_back(new AuthnInfoProvider("whoami"));
  root_->children.push_back(proc);
}


ProcCatalog::~ProcCatalog()
{
  delete root_;
}


std::string ProcCatalog::getImplId() const throw()
{
  return "ProcCatalog";
}


void ProcCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->root_->setStackInstance(si);
  si->set("proc.root", this->root_);
}


void ProcCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
}


void ProcCatalog::changeDir(const std::string& path) throw (DmException)
{
  this->cwd_ = path;
}


std::string ProcCatalog::getWorkingDir(void) throw (DmException)
{
  return this->cwd_;
}


ExtendedStat ProcCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  return ProcProvider::follow(root_, cwd_, path)->stat();
}


Directory* ProcCatalog::openDir(const std::string& path) throw (DmException)
{
  ProcProvider* provider = ProcProvider::follow(root_, cwd_, path);
  ProcDir *dir = new ProcDir();
  dir->provider = provider;
  dir->i = dir->provider->children.begin();
  return dir;
}


void ProcCatalog::closeDir(Directory* dir) throw (DmException)
{
  ProcDir *pdir = static_cast<ProcDir*>(dir);
  delete pdir;
}


struct dirent* ProcCatalog::readDir(Directory* dir) throw (DmException)
{
  ProcDir *pdir = static_cast<ProcDir*>(dir);
  if (pdir->i == pdir->provider->children.end()) {
    return NULL;
  }
  else {
    pdir->xs = (*(pdir->i))->stat();
    pdir->i++;

    ::strncpy(pdir->de.d_name, pdir->xs.name.c_str(), sizeof(pdir->de.d_name));
    pdir->de.d_reclen = pdir->xs.name.size();
    pdir->de.d_type = S_ISDIR(pdir->xs.stat.st_mode)?DT_DIR:DT_REG;

    return &pdir->de;
  }
}


ExtendedStat* ProcCatalog::readDirx(Directory* dir) throw (DmException)
{
  ProcDir *pdir = static_cast<ProcDir*>(dir);
  if (pdir->i == pdir->provider->children.end()) {
    return NULL;
  }
  else {
    pdir->xs = (*(pdir->i))->stat();
    pdir->i++;
    return &pdir->xs;
  }
}


std::vector<Replica> ProcCatalog::getReplicas(const std::string& path) throw (DmException)
{
  std::vector<Replica> replicas;
  Replica replica;

  replica.replicaid = 0;
  replica.fileid = 0;
  replica.nbaccesses = 0;
  replica.atime = replica.ptime = replica.ltime = time(NULL);
  replica.status = Replica::kAvailable;
  replica.type = Replica::kPermanent;

  replica.server.clear();
  replica.rfn = path;
  replica["pool.type"] = std::string("proc");

  replicas.push_back(replica);
  return replicas;
}


ProcPoolManager::ProcPoolManager(): si_(NULL)
{
}


ProcPoolManager::~ProcPoolManager()
{
}


std::string ProcPoolManager::getImplId(void) const throw()
{
  return "ProcPoolManager";
}


void ProcPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}


void ProcPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
}


Location ProcPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  std::vector<Replica> replicas = this->si_->getCatalog()->getReplicas(path);
  if (replicas.empty())
    throw DmException(DMLITE_NO_REPLICAS, "No replicas for %s", path.c_str());
  Replica replica = replicas[0];
  Chunk chunk;
  chunk.url.path = replica.rfn;
  return Location(1, chunk);
}


static void registerPluginProc(PluginManager* pm) throw (DmException)
{
  ProcFactory* pf = new ProcFactory();
  pm->registerCatalogFactory(pf);
  pm->registerPoolManagerFactory(pf);
  pm->registerIOFactory(pf);
}


struct PluginIdCard plugin_proc = {
  PLUGIN_ID_HEADER,
  registerPluginProc
};
