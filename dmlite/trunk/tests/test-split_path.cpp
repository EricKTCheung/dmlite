#include <assert.h>
#include <cstdarg>
#include <iostream>
#include <list>
#include <dmlite/common/Urls.h>

inline int Validate(const char* path, int n, ...)
{
  std::list<std::string> components = dmlite::splitPath(path);
  va_list check;
  bool    failed = false;

  std::cout << "- Checking " << path;

  int i;
  std::list<std::string>::const_iterator j;
  va_start(check, n);
  for (i = 0, j = components.begin(); i < n && j != components.end(); ++i, ++j) {
    const char *c = va_arg(check, const char*);
    if (*j != c) {
      failed = true;
      std::cout << std::endl << "\tExpected '" << c << "' at position " << i
                << ", got '" << *j << "'";
    }
  }
  va_end(check);

  if (j != components.end()) {
    failed = true;
    std::cout << std::endl << "\tThe returned list is bigger!" << std::endl;
  }
  else if (i < n) {
    failed = true;
    std::cout << std::endl << "\tThe returned list is smaller! Got "
              << components.size() << " while expecting " << n << std::endl;
  }

  if (!failed)
    std::cout << "\t[OK]" << std::endl;

  return failed ? 1 : 0;
}

int main(int argc, char **argv)
{
  int r = 0;

  r += Validate("no-root", 1, "no-root");
  r += Validate("/regular", 1, "regular");
  r += Validate("/two/levels", 2, "two", "levels");
  r += Validate("/three/levels/without", 3, "three", "levels", "without");
  r += Validate("/three/levels/slash/", 3, "three", "levels", "slash");
  r += Validate("//several////slashes//////right//", 3, "several", "slashes", "right");

  return r == 0;
}
