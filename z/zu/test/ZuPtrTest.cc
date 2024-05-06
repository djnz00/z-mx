#include <stdlib.h>
#include <stdio.h>

#include <zlib/ZuPtr.hh>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

struct A {
  int *x;
  A(int *x_) : x{x_} { ++*x; }
  ~A() { ++*x; }
};

void foo(A *a) {
  CHECK(*(a->x) == 1);
}

int main()
{
  int i = 0;
  {
    ZuPtr<A> a = new A{&i};
    foo(ZuMv(a));
  }
  CHECK(i == 2);
}
