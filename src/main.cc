#include "../inc/main.h"
#include <iostream>



int main(int argc, char* argv[]) {

  
  Bus<sub1,sub2> bus;

  auto s1 = std::make_shared<sub1>();
  auto s2 = std::make_shared<sub2>();

  bus.subscribe(s1);
  bus.subscribe(s2);


  bus.message<sub1>( 0, 0.1 );
  bus.message<sub2>(0,0.1 );

  //std::this_thread::sleep_for(std::chrono::milliseconds(10000));

  //std::tuple<int, bool, float> bean;

  //constexpr std::size_t b = getIdx<float, decltype(bean)>::value;

  //std::cout << b << std::endl;

  //miniBus<int,  bool, float> bus;
  //bus.insert<int>(0);
  
  std::this_thread::sleep_for(std::chrono::milliseconds(100));


}
