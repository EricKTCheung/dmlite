/// @file   include/dmlite/cpp/utils/checksums.h
/// @brief  Utility methods for checksum handling
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_CHECKSUMS_H
#define DMLITE_CPP_UTILS_CHECKSUMS_H

#include <string>

namespace dmlite {
namespace checksums {

/// To be used internally by the plug-ins that need to deal
/// with the legacy-style stored checksums.
/// @note AD => ADLER32
/// @note CS => UNIXcksum (RFC 3230)
/// @note MD => MD5 (RFC 3230)
/// @note Any other is left as is
std::string fullChecksumName(const std::string& cs);

/// Inverse of fullChecksumName
/// This should eventually disappear, once the backends can deal with
/// full checksum names.
std::string shortChecksumName(const std::string& cs);

}
}

#endif // DMLITE_CPP_UTILS_CHECKSUMS_H
