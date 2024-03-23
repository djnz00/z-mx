#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmAssert.hpp>

#include <iostream>

ZmAtomic<int> baz = 42;

int foo() { return baz; }
int bar() { return ++baz; }

int main()
{
  ZmAssert(foo() == bar() - 1);
  ZmAssert(foo() != bar() - 1);
}
