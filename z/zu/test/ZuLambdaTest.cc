#include <iostream>

#include <zlib/ZuLib.hh>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

static int j;

void foo(int i) {
  CHECK(i == j);
  --j;
}

int main()
{
  j = 2;
  ZuLambda{[i = 2](auto &&self) mutable -> void {
    foo(i);
    if (--i >= 0) self();
  }}();

  j = 2;
  ZuLambda{[](auto &&self, auto I) mutable -> void {
    foo(I);
    enum { J = I - 1 };
    if constexpr (J >= 0) {
      auto next = [self = ZuMv(self)] mutable { ZuMv(self)(ZuInt<J>{}); };
      ZuMv(next)();
    }
  }}(ZuInt<2>{});
}
