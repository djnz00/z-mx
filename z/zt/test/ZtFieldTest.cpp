//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuID.hpp>

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
  ZuID id;
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
    (((string)), (String, "hello \"world\""), (Ctor(0), Quote)),
    (((id)), (String, "goodbye"), (Ctor(1))),
    (((int_)), (Int), (Ctor(2))),
    (((int_ranged)), (Int, 0, 100, 42), (Ctor(3))),
    (((hex)), (Hex, 0xdeadbeef), (Ctor(4))),
    (((enum_)), (Enum, Values::Map), (Ctor(5))),
    (((flags)), (Flags, Flags::Map), (Ctor(6))),
    (((float_)), (Float), (Ctor(7))),
    (((float_ranged)), (Float, 0.0, 1, 0.42), (Ctor(8))),
    (((fixed)), (Fixed), (Ctor(9))),
    (((decimal)), (Decimal), (Ctor(10))),
    (((time_)), (Time), (Ctor(11))));

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
    s << " minimum=" << typename T::Print_{T::minimum(), m.fmt}
      << " maximum=" << typename T::Print_{T::maximum(), m.fmt};
    return s;
  }
};

int main()
{
  using Fields = ZuFieldList<Foo>;
  ZtFieldFmt fmt;
  ZuTypeAll<Fields>::invoke([&fmt]<typename Field>() {
    std::cout << Field::id()
      << " deflt=" << typename Field::Print_{Field::deflt(), fmt}
      << MinMax<Field>{fmt} << '\n';
  });
  std::cout << "double nan=" << __builtin_nan("0") << '\n';
}
