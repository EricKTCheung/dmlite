#include <iostream>
#include <cerrno>
#include <cstdio>
#include <zlib.h>
#include <cstdlib>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

using namespace std;

std::string hexPrinter(const unsigned char* data, size_t nbytes) {
  char  buffer[nbytes * 2 + 1];
  char *p;

  p = buffer;
  for (size_t offset = 0; offset < nbytes; ++offset, p += 2)
    sprintf(p, "%02x", data[offset]);
  *p = '\0';

  return std::string(buffer);
}

int zlib_crc32(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t        nbytes;

  uLong crc = crc32(0L, Z_NULL, 0);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    crc = crc32(crc, buffer, nbytes);
  }

  cout << ">>>>> HASH " << hexPrinter((unsigned char*) &crc, 4) << endl;
  cout << ">>>>> HASH " << crc << endl;
  return 0;
}

int zlib_adler32(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t nbytes;

  uLong adler = adler32(0L, Z_NULL, 0);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    adler = adler32(adler, buffer, nbytes);
  }

  unsigned char output[4];
  output[3] = (adler & 0xFF);
  output[2] = ((adler >> CHAR_BIT) & 0xFF);
  output[1] = ((adler >> (CHAR_BIT * 2)) & 0xFF);
  output[0] = ((adler >> (CHAR_BIT * 3)) & 0xFF);

  cout << ">>>>> HASH " << hexPrinter(output, 4) << endl;
  return 0;
}

int openssl_sha256(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t nbytes;

  SHA256_CTX ctx;
  SHA256_Init(&ctx);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    SHA256_Update(&ctx, buffer, nbytes);
  }
  SHA256_Final(buffer, &ctx);
  cout << ">>>>> HASH " << hexPrinter(buffer, SHA256_DIGEST_LENGTH) << endl;
  return 0;
}

int openssl_md5(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t nbytes;

  MD5_CTX ctx;
  MD5_Init(&ctx);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    MD5_Update(&ctx, buffer, nbytes);
  }
  MD5_Final(buffer, &ctx);
  cout << ">>>>> HASH " << hexPrinter(buffer, MD5_DIGEST_LENGTH) << endl;
  return 0;
}

int openssl_sha1(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t nbytes;

  SHA_CTX ctx;
  SHA1_Init(&ctx);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    SHA1_Update(&ctx, buffer, nbytes);
  }
  SHA1_Final(buffer, &ctx);
  cout << ">>>>> HASH " << hexPrinter(buffer, SHA_DIGEST_LENGTH) << endl;
  return 0;
}

int openssl_sha224(string checksumtype, int fd) {
  unsigned char buffer[2048];
  size_t nbytes;

  SHA256_CTX ctx;
  SHA224_Init(&ctx);
  while( (nbytes = read(fd, buffer, sizeof(buffer))) != 0) {
    SHA224_Update(&ctx, buffer, nbytes);
  }
  SHA224_Final(buffer, &ctx);
  cout << ">>>>> HASH " << hexPrinter(buffer, SHA224_DIGEST_LENGTH) << endl;
  return 0;
}

std::string remove_prefix_if_exists(std::string str, std::string prefix) {
    if(prefix.size() > str.size()) return str;

    if(std::equal(prefix.begin(), prefix.end(), str.begin())) {
      return str.substr(prefix.size(), str.size()-prefix.size());
    }

    return str;
}

int main(int argc, char** argv) {
  if(argc != 3) {
    cout << "Invalid number of arguments. Expected: 3, received: " << argc << endl;
    cout << "Usage: " << argv[0] << " checksumtype filename" << endl;
    return EINVAL;
  }

  string checksumtype = remove_prefix_if_exists(argv[1], "checksum.");
  string filename = argv[2];

  // make sure I can read the file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    cout << "ERROR: Unable to read: " << filename << endl;
    return EINVAL;
  }

  int ret;

  if(checksumtype == "crc32") ret = zlib_crc32(checksumtype, fd);
  else if(checksumtype == "adler32") ret = zlib_adler32(checksumtype, fd);
  else if(checksumtype == "md5") ret = openssl_md5(checksumtype, fd);
  else if(checksumtype == "sha1") ret = openssl_sha1(checksumtype, fd);
  else if(checksumtype == "sha224") ret = openssl_sha224(checksumtype, fd);
  else if(checksumtype == "sha256") ret = openssl_sha256(checksumtype, fd);
  else {
    cout << "ERROR: I don't know how to calculate this checksum: " << checksumtype << endl;
    ret = EINVAL;
  }

  close(fd);
  return ret;
}
