//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZtField.hpp>

namespace Values {
  ZtEnumValues("Values", High, Low, Normal);
}

namespace Flags {
  ZtEnumValues_(Bit0, Bit1, Bit2);
  ZtEnumFlags("Flags", Map, "0", Bit0, "1", Bit1, "2", Bit2);
}

struct Foo {
  const char *string;
  int int_;
  int int_ranged;
  unsigned hex;
  int enum_;
  uint64_t flags;
  double float_;
  double float_ranged;
  ZuFixed fixed;
  ZuDecimal decimal;
  ZmTime time_;
};

ZtFields(Foo,
    (((string)), (String, "hello"), (Ctor(0))),
    (((int_)), (Int), (Ctor(1))),
    (((int_ranged)), (Int, 0, 100, 42), (Ctor(2))),
    (((hex)), (Hex, 0xdeadbeef), (Ctor(3))),
    (((enum_)), (Enum, Values::Map), (Ctor(4))),
    (((flags)), (Flags, Flags::Map), (Ctor(5))),
    (((float_)), (Float), (Ctor(6))),
    (((float_ranged)), (Float, 0.0, 1, 0.42), (Ctor(7))),
    (((fixed)), (Fixed), (Ctor(8))),
    (((decimal)), (Decimal), (Ctor(9))),
    (((time_)), (Time), (Ctor(10))));

template <typename T, typename = void>
struct MinMax {
  const ZtFieldFmt &fmt; // unused
  template <typename S>
  friend S &operator <<(S &s, const MinMax &) { return s; }
};
template <typename T>
struct MinMax<T, decltype(T::minimum(), void())> {
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const MinMax &m) {
    s << " minimum=" << typename T::Print{T::minimum(), m.fmt}
      << " maximum=" << typename T::Print{T::maximum(), m.fmt};
    return s;
  }
};

int main()
{
  using Fields = ZuFieldList<Foo>;
  ZtFieldFmt fmt;
  ZuTypeAll<Fields>::invoke([&fmt]<typename Field>() {
    std::cout << Field::id()
      << " deflt=" << typename Field::Print{Field::deflt(), fmt}
      << MinMax<Field>{fmt} << '\n';
  });
  std::cout << "double nan=" << __builtin_nan("0") << '\n';
}
