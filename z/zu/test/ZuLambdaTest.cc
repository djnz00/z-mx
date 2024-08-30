#include <iostream>

#include <zlib/ZuLib.hh>

int main()
{
  ZuLambda{[i = 10](auto &&self) mutable -> void {
    std::cout << i << '\n';
    if (--i >= 0) self();
  }}();
}
