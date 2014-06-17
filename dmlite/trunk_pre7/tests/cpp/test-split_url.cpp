#include <assert.h>
#include <dmlite/cpp/utils/urls.h>
#include <iostream>
#include <string.h>



inline int Validate(const char* uri,
                    const char* scheme, const char* host, unsigned port,
                    const char* path, const char* query = NULL, const char* reverse = NULL)
{
  dmlite::Url parsed(uri);
  bool        failed = false;

  // Log
  std::cout << "- Checking " << uri;

  // And check it is like it should
  if (parsed.port != port) {
    std::cout << std::endl
              << "\tExpected port '" << port
              << "', got '" << parsed.port << "'";
    failed = true;
  }
  if (parsed.scheme != scheme) {
    std::cout << std::endl
              << "\tExpected scheme '" << scheme
              << "', got '" << parsed.scheme << "'";
    failed = true;
  }
  if (parsed.domain != host) {
    std::cout << std::endl
              << "\tExpected host '" << host
              << "', got '" << parsed.domain << "'";
    failed = true;
  }
  if (parsed.path != path) {
    std::cout << std::endl
              << "\tExpected path '" << path
              << "', got '" << parsed.path << "'";
    failed = true;
  }
  if (query != NULL && parsed.queryToString().compare(query) != 0) {
    std::cout << std::endl
              << "\tExpected query '" << query
              << "', got '" << parsed.queryToString() << "'";
    failed = true;
  }

  if (parsed.toString().compare(reverse?reverse:uri) != 0) {
    std::cout << std::endl
              << "\tExpected full URL '" << (reverse?reverse:uri)
              << "', got '" << parsed.toString() << "'";
    failed = true;
  }

  if (!failed)
    std::cout << "\t[OK]" << std::endl;
  else
    std::cout << std::endl;

  return failed ? 1 : 0;
}



int main(int argc, char **argv)
{
  int r = 0;

  r += Validate("https://something.else.com:443/mypath?query",
                "https", "something.else.com", 443, "/mypath", "query");
  r += Validate("http://something.else.com:/mypath",
                "http", "something.else.com", 0, "/mypath", NULL,
                "http://something.else.com/mypath");
  r += Validate("/path",
                "", "", 0, "/path");
  r += Validate("host:85:/path",
                "", "host", 85, "/path");
  r += Validate("host:/path",
                "", "host", 0, "/path");
  r += Validate("file:///tmp/file",
                "file", "", 0, "/tmp/file");
  r += Validate("file:////tmp/file",
                "file", "", 0, "//tmp/file");
  r += Validate("host-with-hyphen.cern.ch:/scratch/file",
                "", "host-with-hyphen.cern.ch", 0, "/scratch/file");
  r += Validate("http://a.host.com/replica", "http", "a.host.com", 0, "/replica", "");

  r += Validate("https://host.com/file?field=55&flag",
                "https", "host.com", 0, "/file", "field=55&flag");

  r += Validate("https://host.com/file?a=1&b&c=89",
                  "https", "host.com", 0, "/file", "a=1&b&c=89");

  r += Validate("https://dcache-door-atlas19.desy.de:2880/dq2/atlaslocalgroupdisk/user/cmeyer/AlpgenHerwigttbarlnqqNp3/user.cmeyer.AlpgenHerwigttbarlnqqNp3.Nominal.TruthAOD.MZ210312.V1/AlpgenHerwig_Nominal_lnqq_3_00_MZ210312_V1.root",
                "https", "dcache-door-atlas19.desy.de", 2880,
                "/dq2/atlaslocalgroupdisk/user/cmeyer/AlpgenHerwigttbarlnqqNp3/user.cmeyer.AlpgenHerwigttbarlnqqNp3.Nominal.TruthAOD.MZ210312.V1/AlpgenHerwig_Nominal_lnqq_3_00_MZ210312_V1.root",
                "");

  return r;
}
