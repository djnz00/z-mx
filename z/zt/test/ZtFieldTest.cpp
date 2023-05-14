//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZtField.hpp>

namespace Values {
  ZtEnumValues("Values", High, Low, Normal);
}

namespace Flags {
  ZtEnumValues("Flags", Bit0, Bit1, Bit2);
}

struct Foo {
  const char *string;
  int int_;
  int int_ranged;
  unsigned hex;
  int enum_;
  uint64_t flags;
  double float_;
  ZuFixed fixed;
  ZuDecimal decimal;
  ZmTime time_;
};

ZtFields(Foo,
    (((string)), (String, "hello"), (Ctor(0))),
    (((int_)), (Int), (Ctor(1))),
    (((int_ranged)), (Int, 0, 100, 42), (Ctor(2))),
    (((hex)), (Hex), (Ctor(3))),
    (((enum_)), (Enum, Values::Map), (Ctor(4))),
    (((flags)), (Flags, Flags::Map), (Ctor(5))),
    (((float_)), (Float, 0.0, 1, 0.42), (Ctor(6))),
    (((fixed)), (Fixed), (Ctor(7))),
    (((decimal)), (Decimal), (Ctor(8))),
    (((time_)), (Time), (Ctor(9))));

int main()
{
  using Fields = ZuFieldList<Foo>;
  ZuTypeAll<Fields>::invoke([]<typename Field>() {
    std::cout << Field::id() << " deflt=" << Field::deflt() << '\n';
  });

  std::cout << "double nan=" << __builtin_nan("0") << '\n';
}
