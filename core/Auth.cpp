/// @file   core/Auth.cpp
/// @brief  Security API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dm_auth.h>
#include <string.h>

using namespace dmlite;

#define SAFE_STRING(s) s==NULL?"":s


static char* duplicate(const char* s)
{
  if (s == 0x00)
    return 0x00;

  int   l = strlen(s);
  char* p = new char[l + 1];

  strncpy(p, s, l);
  *(p + l) = '\0';

  return p;
}



SecurityCredentials::SecurityCredentials()
{
  this->mech        = 0x00;
  this->client_name = 0x00;
  this->remote_addr = 0x00;
  this->fqans       = 0x00;
  this->nfqans      = 0;
  this->session_id  = 0x00;
  this->cred_data   = 0x00;
}



SecurityCredentials::SecurityCredentials(const Credentials& cred)
{
  this->mech        = duplicate(cred.mech);
  this->client_name = duplicate(cred.client_name);
  this->remote_addr = duplicate(cred.remote_addr);
  this->session_id  = duplicate(cred.session_id);
  this->cred_data   = cred.cred_data;

  this->nfqans = cred.nfqans;
  if (this->nfqans > 0) {
    this->fqans = new const char*[this->nfqans];
    for (unsigned i = 0; i < this->nfqans; ++i) {
      this->fqans[i] = duplicate(cred.fqans[i]);
      this->vfqans.push_back(cred.fqans[i]);
    }
  }
}



SecurityCredentials::SecurityCredentials(const SecurityCredentials& cred)
{
  this->mech        = duplicate(cred.mech);
  this->client_name = duplicate(cred.client_name);
  this->remote_addr = duplicate(cred.remote_addr);
  this->session_id  = duplicate(cred.session_id);
  this->cred_data   = cred.cred_data;

  this->nfqans = cred.nfqans;
  if (this->nfqans > 0) {
    this->fqans = new const char*[this->nfqans];
    for (unsigned i = 0; i < this->nfqans; ++i) {
      this->fqans[i] = duplicate(cred.fqans[i]);
      this->vfqans.push_back(cred.fqans[i]);
    }
  }
}



SecurityCredentials::~SecurityCredentials()
{
  if (this->mech)        delete [] this->mech;
  if (this->client_name) delete [] this->client_name;
  if (this->remote_addr) delete [] this->remote_addr;
  if (this->session_id)  delete [] this->session_id;

  if (this->fqans) {
    for (unsigned i = 0; i < this->nfqans; ++i)
      delete [] this->fqans[i];
    delete [] this->fqans;
  }
}



std::string SecurityCredentials::getClientName() const throw ()
{
  return this->client_name;
}



const std::vector<std::string>& SecurityCredentials::getFqans() const throw()
{
  return this->vfqans;
}



std::string SecurityCredentials::getSecurityMechanism() const throw()
{
  return SAFE_STRING(this->mech);
}



std::string SecurityCredentials::getSessionId() const throw()
{
  return SAFE_STRING(this->session_id);
}



std::string SecurityCredentials::getRemoteAddress() const throw()
{
  return SAFE_STRING(this->remote_addr);
}



const SecurityCredentials& SecurityCredentials::operator = (const SecurityCredentials& cred)
{
  if (this->mech)        delete [] this->mech;
  if (this->client_name) delete [] this->client_name;
  if (this->remote_addr) delete [] this->remote_addr;
  if (this->session_id)  delete [] this->session_id;

  if (this->fqans) {
    for (unsigned i = 0; i < this->nfqans; ++i)
      delete [] this->fqans[i];
    delete [] this->fqans;
  }

  this->mech        = duplicate(cred.mech);
  this->client_name = duplicate(cred.client_name);
  this->remote_addr = duplicate(cred.remote_addr);
  this->session_id  = duplicate(cred.session_id);
  this->cred_data   = cred.cred_data;

  this->nfqans = cred.nfqans;
  if (this->nfqans > 0) {
    this->fqans = new const char*[this->nfqans];
    for (unsigned i = 0; i < this->nfqans; ++i) {
      this->fqans[i] = duplicate(cred.fqans[i]);
      this->vfqans.push_back(cred.fqans[i]);
    }
  }
  else {
    this->fqans = 0x00;
  }

  return *this;
}



SecurityContext::SecurityContext()
{
  memset(&this->user_, 0x00, sizeof(UserInfo));

  GroupInfo rootg;
  memset(&rootg, 0x00, sizeof(GroupInfo));
  groups_.push_back(rootg);
}



SecurityContext::SecurityContext(const SecurityCredentials& cred,
                                 const UserInfo& u,
                                 const std::vector<GroupInfo>& g):
  credentials_(cred), user_(u), groups_(g)
{
  // Push one group at least!
  if (groups_.size() == 0) {
    GroupInfo rootg;
    memset(&rootg, 0x00, sizeof(GroupInfo));
    groups_.push_back(rootg);
  }
}



SecurityContext::~SecurityContext()
{
  // Nothing
}



const SecurityCredentials& SecurityContext::getCredentials() const throw()
{
  return this->credentials_;
}



const GroupInfo& SecurityContext::getGroup(unsigned idx) const throw(DmException)
{
  if (idx > this->groups_.size())
    throw DmException(DM_NO_SUCH_GROUP, "Index out of range");
  return this->groups_[idx];
}



GroupInfo& SecurityContext::getGroup(unsigned idx) throw (DmException)
{
  if (idx > this->groups_.size())
    throw DmException(DM_NO_SUCH_GROUP, "Index out of range");
  return this->groups_[idx];
}



const UserInfo& SecurityContext::getUser() const throw()
{
  return this->user_;
}



UserInfo& SecurityContext::getUser() throw()
{
  return this->user_;
}



unsigned SecurityContext::groupCount() const throw()
{
  return this->groups_.size();
}



void SecurityContext::resizeGroup(unsigned size) throw ()
{
  GroupInfo def;
  memset(&def, 0x00, sizeof(GroupInfo));
  this->groups_.resize(size, def);
}



void SecurityContext::setCredentials(const SecurityCredentials& creds) throw()
{
  this->credentials_ = creds;
}



bool SecurityContext::hasGroup(gid_t gid) const throw()
{
  std::vector<GroupInfo>::const_iterator i;
  for (i = this->groups_.begin(); i != this->groups_.end(); ++i)
    if (i->gid == gid && !i->banned)
      return true;
  return false;
}
