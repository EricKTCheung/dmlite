/*
 * Copyright 2015 CERN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "DomeGenQueue.h"
#include <iostream>

using namespace std;

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define DECLARE_TEST() TestDeclaration __test_declaration(__FUNCTION__)
#define ASSERTm(assertion, msg) \
    if((assertion) == false) throw std::runtime_error( SSTR(__FILE__ << ":" << __LINE__ << " (" << __func__ << "): Assertion " << #assertion << " failed.\n" << msg))
#define ASSERT(assertion) ASSERTm((assertion), "")

class TestDeclaration {
public:
  TestDeclaration(std::string name) {
    std::cout << " ----- Performing test: " << name << std::endl;
  }

  ~TestDeclaration() {
    std::cout << " -- test successful" << std::endl;
  }
};

std::vector<string> v2(string p1, string p2) {
  std::vector<string> ret;
  ret.push_back(p1);
  ret.push_back(p2);
  return ret;
}

std::vector<size_t> simpleLimits() {
  std::vector<size_t> limits;
  limits.push_back(2);
  limits.push_back(1);
  return limits;
}

void test1() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  ASSERT(queue.nTotal() == 0);

  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv1", "pool1"));
  ASSERT(queue.nTotal() == 1);
  ASSERT(queue.nWaiting() == 1);

  GenPrioQueueItem_ptr item = queue.getNextToRun();
  ASSERT(item->namekey == "1");
  ASSERT(item->status == GenPrioQueueItem::Running);
  ASSERT(queue.nTotal() == 1);
  ASSERT(queue.nWaiting() == 0);

  queue.removeItem(item->namekey);
  ASSERT(queue.nTotal() == 0);
  ASSERT(queue.nWaiting() == 0);
}

void test2() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv1", "pool1"));
  queue.touchItemOrCreateNew("2", GenPrioQueueItem::Waiting, 1, v2("srv2", "pool2"));
  queue.touchItemOrCreateNew("3", GenPrioQueueItem::Waiting, 1, v2("srv3", "pool3"));

  GenPrioQueueItem_ptr item2 = queue.getNextToRun();
  GenPrioQueueItem_ptr item3 = queue.getNextToRun();
  GenPrioQueueItem_ptr item1 = queue.getNextToRun();

  ASSERT(item2->namekey == "2");
  ASSERT(item3->namekey == "3");
  ASSERT(item1->namekey == "1");

  ASSERT(queue.nTotal() == 3);
  ASSERT(queue.nWaiting() == 0);

  queue.removeItem(item3->namekey);
  ASSERT(queue.nTotal() == 2);

  queue.removeItem(item1->namekey);
  ASSERT(queue.nTotal() == 1);

  queue.removeItem(item2->namekey);
  ASSERT(queue.nTotal() == 0);
}

void test3() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool"));
  queue.touchItemOrCreateNew("2", GenPrioQueueItem::Waiting, 0, v2("srv", "pool"));

  GenPrioQueueItem_ptr item1 = queue.getNextToRun();
  ASSERT(queue.getNextToRun() == NULL); // hits "pool" limit

  ASSERT(item1 != NULL);
  ASSERT(item1->namekey == "1");
  ASSERT(queue.nWaiting() == 1);

  queue.removeItem(item1->namekey);
  GenPrioQueueItem_ptr item2 = queue.getNextToRun();

  ASSERT(item2->namekey == "2");
  ASSERT(queue.nWaiting() == 0);
}

void test4() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  queue.touchItemOrCreateNew("2", GenPrioQueueItem::Waiting, 2, v2("srv", "pool2"));
  queue.touchItemOrCreateNew("3", GenPrioQueueItem::Waiting, 1, v2("srv", "pool3"));
  queue.touchItemOrCreateNew("4", GenPrioQueueItem::Waiting, 2, v2("srv", "pool2"));

  GenPrioQueueItem_ptr item2 = queue.getNextToRun();
  ASSERT(item2->namekey == "2");

  GenPrioQueueItem_ptr item3 = queue.getNextToRun(); // 4 has higher priority, but hits limit "pool2"
  ASSERT(item3->namekey == "3");

  ASSERT(queue.getNextToRun() == NULL); // hits limit "srv"

  queue.removeItem(item3->namekey);
  GenPrioQueueItem_ptr item1 = queue.getNextToRun(); // 4 has higher priority, but hits limit "pool2"
  ASSERT(item1->namekey == "1");

  ASSERT(queue.getNextToRun() == NULL); // hits limit "srv"

  queue.removeItem(item1->namekey);
  ASSERT(queue.getNextToRun() == NULL); // only 4 left, hits limit "pool2"

  queue.removeItem(item2->namekey);
  GenPrioQueueItem_ptr item4 = queue.getNextToRun();
  ASSERT(item4->namekey == "4");

  ASSERT(queue.nWaiting() == 0);
  ASSERT(queue.nTotal() == 1);

  queue.removeItem(item4->namekey);
  ASSERT(queue.nTotal() == 0);
}

void test5() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Running, 0, v2("srv", "pool1"));
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Running, 0, v2("srv", "pool1"));

  ASSERT(queue.nWaiting() == 0);
  ASSERT(queue.nTotal() == 1);

  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  ASSERT(queue.nWaiting() == 0);
  ASSERT(queue.nTotal() == 1);

  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Finished, 0, v2("srv", "pool1")); // same as removeItem
  ASSERT(queue.nWaiting() == 0);
  ASSERT(queue.nTotal() == 0);

  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Running, 0, v2("srv", "pool1"));
  queue.touchItemOrCreateNew("2", GenPrioQueueItem::Running, 0, v2("srv", "pool2"));
  queue.touchItemOrCreateNew("3", GenPrioQueueItem::Waiting, 0, v2("srv", "pool3"));

  ASSERT(queue.getNextToRun() == NULL); // hits "srv" limit
}

void test6() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));

  ASSERT(queue.removeItem("adf") == NULL);

  GenPrioQueueItem_ptr item1 = queue.getNextToRun();
  ASSERT(item1->namekey == "1");

  // do something unexpected, ie change priority
  // make sure the change is reflected in our item
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 1, v2("srv", "pool1"));
  ASSERT(item1->priority == 1);
}

void test7() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));

  sleep(2);
  queue.tick();
  ASSERT(queue.nTotal() == 0);
}

void test8() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  queue.touchItemOrCreateNew("2", GenPrioQueueItem::Waiting, 1, v2("srv", "pool2"));

  usleep(500000);
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  usleep(500000);
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("srv", "pool1"));
  usleep(1000000);
  queue.tick();

  ASSERT(queue.nTotal() == 1);
  GenPrioQueueItem_ptr item1 = queue.getNextToRun();
  ASSERT(item1->namekey == "1");
}

void test9() {
  DECLARE_TEST();

  GenPrioQueue queue(1, simpleLimits());
  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("123", "234"));
  GenPrioQueueItem_ptr item1 = queue.getNextToRun();
  ASSERT(item1->namekey == "1");
  ASSERT(item1->status == GenPrioQueueItem::Running);

  GenPrioQueueItem_ptr item_null = queue.getNextToRun();
  ASSERT(queue.getNextToRun() == NULL);

  queue.touchItemOrCreateNew("1", GenPrioQueueItem::Waiting, 0, v2("1234", "567"));

  ASSERT(queue.getNextToRun() == NULL);
  queue.tick();
  ASSERT(queue.getNextToRun() == NULL);
}

int main() {
  test1();
  test2();
  test3();
  test4();
  test5();
  test6();
  test7();
  test8();
  test9();
  return 0;
}
