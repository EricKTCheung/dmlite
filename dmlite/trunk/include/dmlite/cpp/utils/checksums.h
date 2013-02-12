/// @file   include/dmlite/cpp/utils/checksums.h
/// @brief  Utility methods for checksum handling
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_CHECKSUMS_H
#define DMLITE_CPP_UTILS_CHECKSUMS_H

#include <dmlite/cpp/io.h>
#include <string>

namespace dmlite {
namespace checksums {

/// To be used internally by the plug-ins that need to deal
/// with the legacy-style stored checksums.
/// @note AD => ADLER32
/// @note CS => CRC32
/// @note MD => MD5 (RFC 3230)
/// @note Any other is left as is
std::string fullChecksumName(const std::string& cs);

/// Inverse of fullChecksumName
/// This should eventually disappear, once the backends can deal with
/// full checksum names.
std::string shortChecksumName(const std::string& cs);

/// Returns the MD5 checksum of the data contained on the IO handler
/// in hexadecimal format.
/// @param io     The IO handler to be digested. The read/write possition will be moved!
/// @param offset Where to start to digest.
/// @param size   The number of bytes to digest. 0 means the whole file.
/// @return       The MD5 checkum in base 16
std::string md5(IOHandler* io, off_t offset = 0, off_t size = 0);

/// Returns the CRC checksum of the data contained on the IO handler (as zlib crc32)
/// in base 10 format.
/// @param io     The IO handler to be digested. The read/write possition will be moved!
/// @param offset Where to start to digest.
/// @param size   The number of bytes to digest. 0 means the whole file.
/// @return       The CRC checkum in base 10
std::string crc32(IOHandler* io, off_t offset = 0, off_t size = 0);

/// Returns the Adler32 checksum of the data contained on the IO handler
/// in hexadecimal format.
/// @param io     The IO handler to be digested. The read/write possition will be moved!
/// @param offset Where to start to digest.
/// @param size   The number of bytes to digest. 0 means the whole file.
/// @return       The Adler32 checkum in base 16
std::string adler32(IOHandler* io, off_t offset = 0, off_t size = 0);

/// Returns the hexadecimal representation of the data
/// @param data   The data to dump to hexadecimal representation.
/// @param nbytes The number of bytes in data
std::string hexPrinter(const unsigned char* data, size_t nbytes);

/// Returns the decimal representation of the data, separated by spaces
/// (num1 num2 num3)
/// @param data   The data to dump to decimal representation.
/// @param nbytes The number of bytes in data
/// @note It assumes data is an array of 'unsigned long'
std::string decPrinter(const unsigned char* data, size_t nbytes);

}
}

#endif // DMLITE_CPP_UTILS_CHECKSUMS_H
