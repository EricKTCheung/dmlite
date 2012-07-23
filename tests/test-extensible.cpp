#include <algorithm>
#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <dmlite/cpp/utils/extensible.h>

#define REMOVE_SPACES(str) str.erase(std::remove_if(str.begin(), str.end(), ::isspace), str.end());

class TestExtensible: public CppUnit::TestFixture {
  public:
    void testRegular()
    {
      dmlite::Extensible ext;
      
      // Empty
      CPPUNIT_ASSERT_EQUAL(false, ext.hasField("something"));
      
      // Put a string, get a string
      ext["something"] = std::string("else");
      CPPUNIT_ASSERT_EQUAL(true, ext.hasField("something"));
      CPPUNIT_ASSERT_EQUAL(std::string("else"), ext.getString("something"));
    }
    
    void testStrings()
    {
      dmlite::Extensible ext;
      
      // Put a const char*, get a string
      const char* cvar = "else";
      ext["something"] =  cvar;
      CPPUNIT_ASSERT_EQUAL(std::string("else"), ext.getString("something"));
      
      // Put a char*, get a string
      char *var = {"value"};
      ext["second"] = var;
      CPPUNIT_ASSERT_EQUAL(std::string("value"), ext.getString("second"));
    }
    
    void testSerialize()
    {
      dmlite::Extensible ext;
      const char* cvar = "old-style";
      
      ext["string"]   = std::string("careful-with-\\scape\"seque\nnces\x04");
      ext["c-string"] = cvar;
      ext["int"]      = 42;
      ext["unsigned"] = 7u;
      ext["float"]    = 5.2f;
      ext["bool"]     = true;
      
      std::string expected("{\
\"bool\": true,\
\"c-string\": \"old-style\",\
\"float\": 5.2,\
\"int\": 42,\
\"string\": \"careful-with-\\\\scape\\\"seque\\nnces\\u0004\",\
\"unsigned\": 7\
}");
      
      std::string got = ext.serialize();
      
      // Remove spaces, since they are not relevant, before comparison
      REMOVE_SPACES(expected);
      REMOVE_SPACES(got);
      CPPUNIT_ASSERT_EQUAL(expected, got);
    }
    
    void testDeserialize()
    {
      dmlite::Extensible ext;
      
      std::string json("{\
\"string\": \"none-\\\"quote\",\
\"int\": 42,\
\"unsigned\": 7,\
\"float\": 5.2,\
\"bool\": true\
}");
      
      ext.deserialize(json);
      
      CPPUNIT_ASSERT_EQUAL(std::string("none-\"quote"), ext.getString("string"));
      CPPUNIT_ASSERT_EQUAL(42l,   ext.getLong("int"));
      CPPUNIT_ASSERT_EQUAL(7lu,   ext.getUnsigned("unsigned"));
      CPPUNIT_ASSERT_EQUAL(5.2,   ext.getDouble("float"));
      CPPUNIT_ASSERT_EQUAL(true,  ext.getBool("bool"));
    }
   
    CPPUNIT_TEST_SUITE(TestExtensible);
    CPPUNIT_TEST(testRegular);
    CPPUNIT_TEST(testStrings);
    CPPUNIT_TEST(testSerialize);
    CPPUNIT_TEST(testDeserialize);
    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestExtensible);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run()?0:1;
}
