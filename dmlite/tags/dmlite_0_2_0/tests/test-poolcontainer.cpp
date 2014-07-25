#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <dmlite/common/PoolContainer.h>

using namespace dmlite;

struct Dummy {
  int  id;
  bool valid;
};

class DummyFactory:public PoolElementFactory<Dummy*> {
public:
  int  id, lastDestroyed;
  bool valid;



  DummyFactory(): id(0), lastDestroyed(-1), valid(true) {}


  Dummy* create()
  {
    Dummy* d = new Dummy();
    d->id    = id++;
    d->valid = valid;
    return d;
  }



  void destroy(Dummy* c)
  {
    lastDestroyed = c->id;
    delete c;
  }



  bool isValid(Dummy* c)
  {
    return c->valid;
  }
};


class PoolContainerTest: public CppUnit::TestFixture {
protected:
  PoolContainer<Dummy*> *pool;
  DummyFactory *factory;
public:

  void setUp(void)
  {
    factory = new DummyFactory();
    pool    = new PoolContainer<Dummy*>(factory, 2);
  }



  void tearDown(void)
  {
    delete pool;
    delete factory;
  }



  void testOne(void)
  {
    Dummy *d;
    d = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d->id);
    pool->release(d);
    d = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d->id);
    pool->release(d);
  }


  void testTwo(void)
  {
    Dummy *d, *d2, *d3;
    d = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d->id);
    d2 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(1, d2->id);
    pool->release(d);
    d3 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d3->id);

    pool->release(d2);
    pool->release(d3);
  }



  void testInvalid(void)
  {
    Dummy *d;
    d = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d->id);
    CPPUNIT_ASSERT_EQUAL(true, d->valid);
    d->valid = false;
    pool->release(d);
    d = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(1, d->id);
    pool->release(d);
  }



  void testResize(void)
  {
    Dummy *d1, *d2, *d3;
    // Take the two
    d1 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(0, d1->id);
    d2 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(1, d2->id);
    // Resize
    pool->resize(3);
    // Take third
    d3 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(2, d3->id);
    // Resize to smaller
    pool->resize(1);
    pool->release(d3);
    pool->release(d2);
    CPPUNIT_ASSERT_EQUAL(1, factory->lastDestroyed);
    pool->release(d1);
    CPPUNIT_ASSERT_EQUAL(0, factory->lastDestroyed);
    // Now it should be the first freed
    d1 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(2, d1->id);
    // Bigger to allow a new one
    pool->resize(2);
    // Must be a new one
    d2 = pool->acquire();
    CPPUNIT_ASSERT_EQUAL(3, d2->id);
    // Release
    pool->release(d1);
    pool->release(d2);
  }



  void testNoMore()
  {
    Dummy *d1, *d2, *d3;
    d1 = pool->acquire();
    d2 = pool->acquire();
    CPPUNIT_ASSERT_THROW(d3 = pool->acquire(false), dmlite::DmException);
    pool->release(d1);
    pool->release(d2);
  }

  CPPUNIT_TEST_SUITE(PoolContainerTest);
  CPPUNIT_TEST(testOne);
  CPPUNIT_TEST(testTwo);
  CPPUNIT_TEST(testInvalid);
  CPPUNIT_TEST(testResize);
  CPPUNIT_TEST(testNoMore);
  CPPUNIT_TEST_SUITE_END();
};


CPPUNIT_TEST_SUITE_REGISTRATION(PoolContainerTest);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run();
}