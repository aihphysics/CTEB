#include "../inc/main.h"
#include <iostream>

int main(int argc, char* argv[]) {

  // could be databases, http clients, etc
  Bus<sub1,sub2> bus;

  // creation of objects, can be held elsewhere
  auto s1 = std::make_shared<sub1>();
  auto s2 = std::make_shared<sub2>();

  // subscribers applied piecemeal
  bus.subscribe(s1);
  bus.subscribe(s2);

  // initiate bus
  bus.start();

  // try some messaging
  bus.message<sub1>(0, 0.1);
  bus.message<sub2>(0,0.1);
  
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::cout << s1->accepted << std::endl;
  std::cout << s2->accepted << std::endl;

  // exit out
}
