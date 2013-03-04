#include <algorithm>
#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cstring>
#include <dmlite/cpp/pooldriver.h>

#define REMOVE_SPACES(str) str.erase(std::remove_if(str.begin(), str.end(), ::isspace), str.end());


std::ostream& operator<< (std::ostream& os, const dmlite::Url& url)
{
  return os << url.toString();
}



std::ostream& operator<< (std::ostream& os, const dmlite::Location& loc)
{
  return os << loc.toString();
}



class TestLocation: public CppUnit::TestFixture {
  public:
    void testChunk() {
      std::string expected, value;

      dmlite::Url url("https://host.domain:443/path/file?query=value");
      dmlite::Chunk chunk;

      chunk.url    = url;
      chunk.offset = 42;
      chunk.size   = 1024;

      // Check serialization
      expected = "{\"url\": \"https:\\/\\/host.domain:443\\/path\\/file?query=value\", \"offset\": 42, \"size\": 1024}";
      value = chunk.toString();
      REMOVE_SPACES(expected);
      REMOVE_SPACES(value);
      CPPUNIT_ASSERT_EQUAL(expected, value);

      // Check deserialization
      dmlite::Chunk deserialized(expected);
      CPPUNIT_ASSERT_EQUAL(url,            deserialized.url);
      CPPUNIT_ASSERT_EQUAL((uint64_t)42,   deserialized.offset);
      CPPUNIT_ASSERT_EQUAL((uint64_t)1024, deserialized.size);
    }

    void testLocation() {
      std::string expected, value;

      // Add a couple of Chunks
      dmlite::Location location;
      location.push_back(dmlite::Chunk("https://host.domain:443/path/file?token=something", 0, 1024));
      location.push_back(dmlite::Chunk("https://host2.domain:443/path/file?token=something-else", 1024, 2048));

      // Check serialization
      expected = "\
{\"chunks\": [\
    {\
        \"url\": \"https:\\/\\/host.domain:443\\/path\\/file?token=something\",\
        \"offset\": 0,\
        \"size\": 1024\
    },\
    {\
        \"url\": \"https:\\/\\/host2.domain:443\\/path\\/file?token=something-else\",\
        \"offset\": 1024,\
        \"size\": 2048\
    }\
]}";
      value    = location.toString();
      REMOVE_SPACES(expected);
      REMOVE_SPACES(value);
      CPPUNIT_ASSERT_EQUAL(expected, value);

      // Check deserialization
      dmlite::Location deserialized(expected);
      CPPUNIT_ASSERT_EQUAL(location.size(), deserialized.size());
      CPPUNIT_ASSERT_EQUAL(location, deserialized);
    }

    void testParameterEscaping()
    {
      dmlite::Url url;

      url.domain = "example.com";
      url.path   = "/path/file";
      url.port   = 8443;
      url.scheme = "https";
      url.query["referrer"] = std::string("https://caller.com/path?param=value");

      std::string serialized = url.toString();

      std::string expected("https://example.com:8443/path/file?referrer=https%3A%2F%2Fcaller.com%2Fpath%3Fparam%3Dvalue");
      CPPUNIT_ASSERT_EQUAL(expected, serialized);

      dmlite::Url check(serialized);

      CPPUNIT_ASSERT_EQUAL(url.query.getString("referrer"),
                           check.query.getString("referrer"));
    }

    void testParameterEscapingUnicode()
    {
      dmlite::Url url;

      url.domain = "example.com";
      url.path   = "/path/file";
      url.port   = 8443;
      url.scheme = "https";
      url.query["referrer"] = std::string("https://caller.com/path?param=ǘëñ");

      std::string serialized = url.toString();

      std::string expected("https://example.com:8443/path/file?referrer=https%3A%2F%2Fcaller.com%2Fpath%3Fparam%3D%C7%98%C3%AB%C3%B1");
      CPPUNIT_ASSERT_EQUAL(expected, serialized);

      dmlite::Url check(serialized);

      CPPUNIT_ASSERT_EQUAL(url.query.getString("referrer"),
                           check.query.getString("referrer"));
    }

    CPPUNIT_TEST_SUITE(TestLocation);
    CPPUNIT_TEST(testChunk);
    CPPUNIT_TEST(testLocation);
    CPPUNIT_TEST(testParameterEscaping);
    CPPUNIT_TEST(testParameterEscapingUnicode);
    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestLocation);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run()?0:1;
}
