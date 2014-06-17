#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <dmlite/cpp/utils/urls.h>
#include <string>

class TestUrlString: public CppUnit::TestFixture
{
public:

  // create a url, convert to string, then back to url and compare with original
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
    // Normalizing the path removing extra slashes may look like it makes sense,
    // but for xrootd, for instance, root://host/path != root://host//path
    CPPUNIT_ASSERT_EQUAL(std::string("/") + p, url2.path);
    CPPUNIT_ASSERT_EQUAL(q1, url2.query.getString("q.1"));
    CPPUNIT_ASSERT_EQUAL(q2, url2.query.getString("q.2"));
  }


  CPPUNIT_TEST_SUITE(TestUrlString);
  CPPUNIT_TEST(testUrlString);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestUrlString);

int main(int argn, char **argv)
{
    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );
    return runner.run()?0:1;
}
