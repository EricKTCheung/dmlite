#include "DomeGenQueue.h"
#include <iostream>

using namespace std;

#define DECLARE_TEST() std::cout << " ----- Performing test: " << __FUNCTION__ << std::endl

void test1() {
  DECLARE_TEST();

  std::vector<size_t> limits;
  limits.push_back(2);
  limits.push_back(1);

  GenPrioQueue queue(1, limits);

}

int main() {
  test1();
  return 0;
}
