#include <boost/algorithm/string.hpp>
#include <dmlite/cpp/utils/checksums.h>
#include <limits.h>
#include <openssl/evp.h>
#include <zlib.h>


std::string dmlite::checksums::fullChecksumName(const std::string& cs)
{
  if (boost::iequals(cs, "AD"))
    return "adler32";
  else if (boost::iequals(cs, "CS"))
    return "crc32";
  else if (boost::iequals(cs, "MD"))
    return "md5";
  else
    return cs;
}



std::string dmlite::checksums::shortChecksumName(const std::string& cs)
{
  if (boost::iequals(cs, "ADLER32"))
    return "AD";
  else if (boost::iequals(cs, "CRC32"))
    return "CS";
  else if (boost::iequals(cs, "MD5"))
    return "MD";
  else
    return cs;
}



typedef void (*UpdateCallback)(const unsigned char* buffer, size_t nbytes, void* udata);
typedef void (*FinalCallback)(unsigned char* output, size_t* outputSize, void* udata);
typedef std::string (*ToStringCallback)(const unsigned char* buffer, size_t nbytes);



std::string dmlite::checksums::hexPrinter(const unsigned char* data, size_t nbytes)
{
  char  buffer[nbytes * 2 + 1];
  char *p;

  p = buffer;
  for (size_t offset = 0; offset < nbytes; ++offset, p += 2)
    sprintf(p, "%02x", data[offset]);
  *p = '\0';

  return std::string(buffer);
}



std::string dmlite::checksums::decPrinter(const unsigned char* data, size_t nbytes)
{
  char   buffer[1024];
  char  *p;
  size_t bufSize = sizeof(buffer);

  p = buffer;
  for (size_t offset = 0; offset < nbytes; offset += sizeof(unsigned long)) {
    unsigned long v;

    ::memcpy(&v, data + offset, sizeof(unsigned long));

    int nchars       = snprintf(p, bufSize, "%lu ", v);
    bufSize -= nchars;
    p       += nchars;
  }
  *(p - 1) = '\0';

  return std::string(buffer);
}



std::string digest(dmlite::IOHandler* io, off_t offset, off_t size,
                   UpdateCallback update, FinalCallback final,
                   ToStringCallback strify,
                   void* udata)
{
  unsigned char buffer[2048];
  size_t        nbytes;


  io->seek(offset, dmlite::IOHandler::kSet);

  if (size > 0) {
    nbytes = (size < (off_t)sizeof(buffer))?size:sizeof(buffer);
    while ((nbytes = io->read((char*)buffer, nbytes))) {
      update(buffer, nbytes, udata);
      size -= nbytes;
      nbytes = (size < (off_t)sizeof(buffer))?size:sizeof(buffer);
    }
  }
  else {
    while ((nbytes = io->read((char*)buffer, sizeof(buffer))))
      update(buffer, nbytes, udata);
  }

  nbytes = sizeof(buffer);
  final(buffer, &nbytes, udata);

  return strify(buffer, nbytes);
}



static void md5DigestUpdate(const unsigned char* buffer, size_t nbytes, void* udata)
{
  EVP_MD_CTX* ctx = static_cast<EVP_MD_CTX*>(udata);
  EVP_DigestUpdate(ctx, buffer, nbytes);
}



static void md5DigestFinal(unsigned char* output, size_t* osize, void* udata)
{
  EVP_MD_CTX* ctx = static_cast<EVP_MD_CTX*>(udata);
  unsigned uOsize = *osize;
  EVP_DigestFinal(ctx, output, &uOsize);
  *osize = uOsize;
}



std::string dmlite::checksums::md5(IOHandler* io, off_t offset, off_t size)
{
  EVP_MD_CTX ctx;

  EVP_MD_CTX_init(&ctx);
  EVP_DigestInit(&ctx, EVP_md5());

  return digest(io, offset, size,
                md5DigestUpdate, md5DigestFinal,
                hexPrinter,
                &ctx);
}



static void crc32DigestUpdate(const unsigned char* buffer, size_t nbytes, void* udata)
{
  unsigned long *crc = static_cast<unsigned long*>(udata);
  *crc = ::crc32(*crc, buffer, nbytes);
}



static void crc32DigestFinal(unsigned char* output, size_t* osize, void* udata)
{
  unsigned long *crc = static_cast<unsigned long*>(udata);
  ::memcpy(output, crc, sizeof(unsigned long));
  *osize = sizeof(unsigned long);
}



std::string dmlite::checksums::crc32(IOHandler* io, off_t offset, off_t size)
{
  unsigned long crc = ::crc32(0l, Z_NULL, 0);

  return digest(io, offset, size,
                crc32DigestUpdate, crc32DigestFinal,
                decPrinter,
                &crc);
}



static void adler32DigestUpdate(const unsigned char* buffer, size_t nbytes, void* udata)
{
  unsigned long *adler = static_cast<unsigned long*>(udata);
  *adler = ::adler32(*adler, buffer, nbytes);
}



static void adler32DigestFinal(unsigned char* output, size_t* osize, void* udata)
{
  unsigned long *adler = static_cast<unsigned long*>(udata);
  unsigned long v      = *adler;

  output[3] = (v & 0xFF);
  output[2] = ((v >> CHAR_BIT) & 0xFF);
  output[1] = ((v >> (CHAR_BIT * 2)) & 0xFF);
  output[0] = ((v >> (CHAR_BIT * 3)) & 0xFF);

  *osize = 4;
}



std::string dmlite::checksums::adler32(IOHandler* io, off_t offset, off_t size)
{
  unsigned long adler = ::adler32(0L, Z_NULL, 0);
  return digest(io, offset, size,
                adler32DigestUpdate, adler32DigestFinal,
                hexPrinter,
                &adler);
}
