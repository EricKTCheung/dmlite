/// @file    include/dmlite/cpp/utils/security.h
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_SECURITY_H_
#define DMLITE_CPP_UTILS_SECURITY_H_

#include <stdint.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include "../authn.h"
#include "../exceptions.h"

namespace dmlite {
  
  /// Possible outputs for validateToken
  enum TokenResult {
    kTokenOK = 0,
    kTokenMalformed,
    kTokenInvalid,
    kTokenExpired,
    kTokenInvalidMode,
    kTokenInternalError
  };
  
  /// ACL Entry
  struct AclEntry {
    /// ACL Type possible values
    static const uint8_t kUserObj  = 1;
    static const uint8_t kUser     = 2;
    static const uint8_t kGroupObj = 3;
    static const uint8_t kGroup    = 4;
    static const uint8_t kMask     = 5;
    static const uint8_t kOther    = 6;
    static const uint8_t kDefault  = 0x20;
                 
    uint8_t  type;
    uint8_t  perm;
    uint32_t id;
    
    // Operators
    bool operator == (const AclEntry&) const;
    bool operator != (const AclEntry&) const;
    bool operator <  (const AclEntry&) const;
    bool operator >  (const AclEntry&) const;
  };
  
  struct Acl: public std::vector<AclEntry> {
   public:
     Acl() throw ();
     
     /// Creates an ACL from a string
     explicit Acl(const std::string&) throw ();
     
     /// Creates a new ACL inheriting from parent.
     /// @param parent The parent's ACL vector.
     /// @param uid    The current user uid.
     /// @param gid    The current user gid.
     /// @param cmode  The creation mode.
     /// @param fmode  The current file mode. It will be modified to fit the inheritance.
     Acl(const Acl& parent, uid_t uid, gid_t gid, mode_t cmode, mode_t* fmode) throw ();
     
     /// Returns the position if there is an ACL entry with the type 'type'
     /// -1 otherwise.
     int has(uint8_t type) const throw ();
     
     std::string serialize(void) const throw ();
     void        validate (void) const throw (DmException);
  };
  
  /// Check if the group vector contains the given gid.
  /// @param groups The GroupInfo vector.
  /// @param gid    The gid to look for.
  /// @return       true if the vector contains the given gid. false otherwise.
  bool hasGroup(const std::vector<GroupInfo>& groups, gid_t gid);

  /// Check if a specific user has the demanded rights.
  /// @note This works using uid and gid, so it will only work with plug-ins that
  ///       provide this metadata (as unsigned!!).
  /// @param context The security context.
  /// @param acl     The Access Control list.
  /// @param stat    A struct stat which mode will be checked.
  /// @param mode    The mode to be checked.
  /// @return        0 if the mode is allowed, 1 if not.
  int checkPermissions(const SecurityContext* context,
                       const Acl& acl, const struct stat& stat,
                       mode_t mode);

  /// Get the VO from a full DN.
  /// @param mapfile The file that contains the user => group mapping.
  /// @param dn      The DN to parse.
  /// @return        The mapped VO.
  std::string voFromDn(const std::string& mapfile, const std::string& dn);

  /// Get the VO from a role.
  /// @param role The role.
  /// @return     The VO.
  std::string voFromRole(const std::string& role);

  /// Get the host DN from the host certificate
  std::string getHostDN(void);

  /// Generate a token.
  /// @param id       A unique ID of the user. May be the DN, the IP...
  /// @param pfn      The PFN we want a token for.
  /// @param passwd   The password to be used.
  /// @param lifetime Token lifetime.
  /// @param write    If true, this will be a token for write access.
  std::string generateToken(const std::string& id, const std::string& pfn,
                            const std::string& passwd, time_t lifetime,
                            bool write = false);

  /// Validate a token. It must have been previously generated by generateToken.
  /// @param token  The token to validate.
  /// @param id     The SAME unique ID used to generate the token.
  /// @param pfn    The that is being accessed.
  /// @param passwd The password that must be used to generate the token.
  /// @param write  If true, write access will be validated.
  TokenResult validateToken(const std::string& token, const std::string& id,
                            const std::string& pfn, const std::string& passwd,
                            bool write = false);

};

#endif // DMLITE_CPP_UTILS_SECURITY_H_
