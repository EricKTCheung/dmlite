/// @file   DomeAdapterCatalog.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <iostream>

#include "utils/DavixPool.h"
#include "DomeAdapterCatalog.h"

using namespace dmlite;

DomeAdapterCatalog::DomeAdapterCatalog() throw (DmException) {
}

std::string DomeAdapterCatalog::getImplId() const throw() {
  return std::string("DomeAdapterCatalog");
}

void DomeAdapterCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}

void DomeAdapterCatalog::setSecurityContext(const SecurityContext* secCtx) throw (DmException)
{
  this->secCtx_ = secCtx;
}

void DomeAdapterCatalog::changeDir(const std::string& dir) throw (DmException)
{
  std::cout << "change dir to: " << dir << std::endl;
  cwd_ = dir;
}

std::string DomeAdapterCatalog::getWorkingDir(void) throw (DmException) {
  std::cout << "get working dir: " << cwd_ << std::endl;
  return cwd_;
}

Directory* DomeAdapterCatalog::openDir(const std::string& path) throw (DmException) {
  std::cout << "open dir: " << path << std::endl;
  return new DomeDir(path);
}

void DomeAdapterCatalog::closeDir(Directory* dir) throw (DmException) {
  std::cout << "close dir called" << std::endl;
  delete dir;
}

struct dirent* DomeAdapterCatalog::readDir(Directory* dir) throw (DmException) {
  std::cout << "readDir called" << std::endl;
  return NULL;
}

ExtendedStat* DomeAdapterCatalog::readDirx(Directory* dir) throw (DmException) {
  DomeDir *domedir = dynamic_cast<DomeDir*>(dir);

  Davix::Uri uri("https://domehead-trunk.cern.ch");
  Davix::DavixError* tmp_err = NULL;
  DavixStuff *ds = DavixCtxPoolHolder::getDavixCtxPool().acquire();
  Davix::GetRequest req(*ds->ctx, uri, &tmp_err);

  std::cout << "readDirx called on " << domedir->path_ << std::endl;
  return NULL;
}


