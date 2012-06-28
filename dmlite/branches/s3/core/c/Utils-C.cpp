#include <dmlite/c/dm_utils.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/utils/dm_urls.h>

void dm_parse_url(const char* source, struct url* dest)
{
  *dest = dmlite::splitUrl(source);
}



void dm_serialize_acls(int nAcls, struct dm_acl* acls, char* buffer, size_t bsize)
{
  std::vector<Acl> aclV(nAcls);
  aclV.assign(acls, acls + nAcls);

  std::string aclStr = dmlite::serializeAcl(aclV);

  std::strncpy(buffer, aclStr.c_str(), bsize);
}

void dm_deserialize_acls(const char* buffer, int* nAcls, struct dm_acl** acls)
{
  std::vector<Acl> aclV = dmlite::deserializeAcl(buffer);

  *nAcls = aclV.size();
  *acls  = new Acl[*nAcls];

  std::copy(aclV.begin(), aclV.end(), *acls);
}



void dm_freeacls(int nAcls, struct dm_acl* acls)
{
  delete [] acls;
}

