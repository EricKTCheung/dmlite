#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/utils/urls.h>
#include <string>
#include "test-base.h"

class TestUrlString: public TestBase
{
public:
  void setUp()
  {
    TestBase::setUp();
  }

  void tearDown()
  {
    TestBase::tearDown();
  }


  // crate a url, convert to string, then back to url and compare with original
  void testUrlString(void)
  {
    std::string p = "/Apath1!@#%^_-+=:./file.dat";
    dmlite::Url myurl("root://myhost.example.com:1095/" + p);
    std::string q1 = "a!b@C1#d?e=f$g &h%i^j*k.-+";
    std::string q2 = "l(m*n^o%p?q$/r_&s#t@u!v=w";
    myurl.query["q.1"] = q1;
    myurl.query["q.2"] = q2;
    std::string s = myurl.toString();
    dmlite::Url url2(s);
    CPPUNIT_ASSERT_EQUAL(url2.path, p);
    CPPUNIT_ASSERT_EQUAL(url2.query.getString("q.1"), q1);
    CPPUNIT_ASSERT_EQUAL(url2.query.getString("q.2"), q2);
  }


  CPPUNIT_TEST_SUITE(TestUrlString);
  CPPUNIT_TEST(testUrlString);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestUrlString);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
