/// @file   core/Operators.cpp
/// @brief  Implementation of the operators defined for the simple data structs.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <boost/property_tree/json_parser.hpp>
#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/inode.h>
#include <dmlite/cpp/pooldriver.h>
#include <dmlite/cpp/poolmanager.h>

using namespace dmlite;

// Authn

bool SecurityCredentials::operator == (const SecurityCredentials& sc) const
{
  return  this->clientName    == sc.clientName &&
          this->fqans         == sc.fqans &&
          this->mech          == sc.mech &&
          this->remoteAddress == sc.remoteAddress &&
          this->sessionId     == sc.sessionId;
}



bool SecurityCredentials::operator != (const SecurityCredentials& sc) const
{
  return !(*this == sc);
}



bool SecurityCredentials::operator <  (const SecurityCredentials& sc) const
{
  if (this->clientName < sc.clientName)
    return true;
  else if (this->clientName > sc.clientName)
    return false;
  else if (this->fqans < sc.fqans)
    return true;
  else if (this->fqans > sc.fqans)
    return false;
  else
    return this->sessionId < sc.sessionId;
}



bool SecurityCredentials::operator >  (const SecurityCredentials& sc) const
{
  return (*this < sc);
}



bool UserInfo::operator == (const UserInfo& u) const
{
  return this->name == u.name;
}



bool UserInfo::operator != (const UserInfo& u) const
{
  return this->name != u.name;
}



bool UserInfo::operator <  (const UserInfo& u) const
{
  return this->name < u.name;
}


bool UserInfo::operator >  (const UserInfo& u) const
{
  return this->name > u.name;
}


bool GroupInfo::operator == (const GroupInfo& g) const
{
  return this->name == g.name;
}



bool GroupInfo::operator != (const GroupInfo& g) const
{
  return this->name != g.name;
}



bool GroupInfo::operator <  (const GroupInfo& g) const
{
  return this->name < g.name;
}



bool GroupInfo::operator >  (const GroupInfo& g) const
{
  return this->name > g.name;
}



bool SecurityContext::operator == (const SecurityContext& s) const
{
  return this->user == s.user && this->groups == s.groups;
}



bool SecurityContext::operator != (const SecurityContext& s) const
{
  return !(*this == s);
}



bool SecurityContext::operator <  (const SecurityContext& s) const
{
  return (this->user < s.user) ||
         (this->user == s.user && this->groups < s.groups);
}



bool SecurityContext::operator >  (const SecurityContext& s) const
{
  return *this < s;
}

// Inode

bool ExtendedStat::operator == (const ExtendedStat& xs) const
{
  return this->stat.st_ino == xs.stat.st_ino;
}



bool ExtendedStat::operator != (const ExtendedStat& xs) const
{
  return !(*this == xs);
}



bool ExtendedStat::operator <  (const ExtendedStat& xs) const
{
  return this->stat.st_ino < xs.stat.st_ino;
}



bool ExtendedStat::operator >  (const ExtendedStat& xs) const
{
  return *this < xs;
}


bool SymLink::operator == (const SymLink& l) const
{
  return this->inode == l.inode;
}



bool SymLink::operator != (const SymLink& l) const
{
  return !(*this == l);
}



bool SymLink::operator <  (const SymLink& l) const
{
  return this->inode < l.inode;
}



bool SymLink::operator >  (const SymLink& l) const
{
  return *this < l;
}



bool Replica::operator == (const Replica& r) const
{
  return this->replicaid == r.replicaid;
}



bool Replica::operator != (const Replica& r) const
{
  return !(*this == r);
}



bool Replica::operator <  (const Replica& r) const
{
  return this->replicaid < r.replicaid;
}


bool Replica::operator >  (const Replica& r) const
{
  return *this < r;
}

// Chunk
Chunk::Chunk(): offset(0), size(0), url()
{
}



Chunk::Chunk(const std::string& url_, uint64_t offset_, uint64_t size_):
    offset(offset_), size(size_), url(url_)
{
}



Chunk::Chunk(const std::string& str)
{
  Extensible helper;
  helper.deserialize(str);
  this->offset = helper.getU64("offset");
  this->size   = helper.getU64("size");
  this->url    = Url(helper.getString("url"));
}



bool Chunk::operator == (const Chunk& c) const
{
  return !(*this < c) && !(*this > c);
}



bool Chunk::operator != (const Chunk& c) const
{
  return !(*this == c);
}



bool Chunk::operator <  (const Chunk& c) const
{
  if (this->url < c.url)
    return true;
  else if (this->url > c.url)
    return false;
  else if (this->offset < c.offset)
    return true;
  else if (this->offset > c.offset)
    return false;
  else
    return this->size < c.size;
}



bool Chunk::operator >  (const Chunk& c) const
{
  return *this < c;
}



std::string Chunk::toString(void) const
{
  Extensible helper;
  helper["url"]    = this->url.toString();
  helper["offset"] = this->offset;
  helper["size"]   = this->size;
  return helper.serialize();
}

// Location
Location::Location(const std::string& str)
{
  Extensible helper;
  helper.deserialize(str);
  std::vector<boost::any> v = helper.getVector("chunks");
  std::vector<boost::any>::const_iterator i;
  for (i = v.begin(); i != v.end(); ++i) {
    try {
      Extensible e = boost::any_cast<Extensible>(*i);
      this->push_back(Chunk(e.getString("url"), e.getU64("offset"), e.getU64("size")));
    }
    catch (const boost::bad_any_cast& e) {
      throw DmException(EINVAL, "Location can not deserialize the string");
    }
  }
}



std::string Location::toString(void) const
{
  std::ostringstream os;

  os << "{\"chunks\": [";
  unsigned i;
  for (i = 0; i < this->size() - 1; ++i)
    os << this->at(i).toString() << ",";
  os << this->at(i).toString() << "]}";

  return os.str();
}

// Pool Manager

bool Pool::operator == (const Pool& p) const
{
  return this->type == p.type && this->name == p.name;
}



bool Pool::operator != (const Pool& p) const
{
  return !(*this == p);
}



bool Pool::operator <  (const Pool& p) const
{
  if (this->type < p.type)
    return true;
  else if (this->type > p.type)
    return false;
  else
    return this->name == p.name;
}



bool Pool::operator >  (const Pool& p) const
{
  return *this < p;
}
