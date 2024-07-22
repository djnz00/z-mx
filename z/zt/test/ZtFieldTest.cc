//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuID.hh>

#include <zlib/ZmDemangle.hh>

#include <zlib/ZtField.hh>

namespace Values {
  ZtEnumValues(Values, int8_t, High, Low, Normal);
}

namespace Flags {
  ZtEnumFlags(Flags, uint8_t, Bit0, Bit1, Bit2);
}

struct Nested {
  int i1, i2;

  friend ZtFieldPrint ZuPrintType(Nested *);
};

ZtFieldTbl(Nested,
  (((i1), (Ctor<0>)), (Int32)),
  (((i2), (Ctor<1>)), (Int32)));

struct Foo {
  const char *string = nullptr;
  ZtArray<uint8_t> bytes;
  ZuID id = "goodbye";
  int int_ = 0;
  int int_ranged = 42;
  unsigned hex = 0xdeadbeef;
  int enum_ = Values::Normal;
  uint128_t flags = Flags::Bit1();
  double float_ = ZuCmp<double>::null();
  double float_ranged = 0.42;
  ZuFixed fixed;
  ZuDecimal decimal;
  ZuTime time_;
  Nested nested;
  ZtArray<ZtArray<uint8_t>> bytesVec;

  friend ZtFieldPrint ZuPrintType(Foo *);
};

ZtFieldTbl(Foo,
    (((string, Rd), (Ctor<0>)), (CString, "hello \"world\"")),
    (((bytes), (Ctor<1>)), (Bytes, ZuBytes{"bytes"})),
    (((id), (Ctor<2>)), (String, "goodbye")),
    (((int_), (Ctor<3>)), (Int32)),
    (((int_ranged), (Ctor<4>)), (Int32, 42, 0, 100)),
    (((hex), (Ctor<5>, Hex)), (UInt32, 0xdeadbeef)),
    (((enum_), (Ctor<6>, Enum<Values::Map>)), (Int32, Values::Normal)),
    (((flags), (Ctor<7>, Flags<Flags::Map>)), (UInt128, Flags::Bit1())),
    (((float_), (Ctor<8>)), (Float)),
    (((float_ranged), (Ctor<9>)), (Float, 0.42, 0.0, 1)),
    (((fixed), (Ctor<10>)), (Fixed)),
    (((decimal), (Ctor<11>)), (Decimal)),
    (((time_), (Ctor<12>)), (Time)),
    (((nested), (Ctor<13>)), (UDT)),
    (((bytesVec), (Ctor<14>)), (BytesVec)));

template <typename T, typename = void>
struct MinMax {
  template <typename S>
  friend S &operator <<(S &s, const MinMax &) { return s; }
};
template <typename T>
struct MinMax<T, decltype(T::minimum(), void())> {
  template <typename S>
  friend S &operator <<(S &s, const MinMax &m) {
    s << " minimum=" << typename T::template Print_<>{T::minimum()}
      << " maximum=" << typename T::template Print_<>{T::maximum()};
    return s;
  }
};

int main()
{
  using Fields = ZuFields<Foo>;

  ZuUnroll::all<Fields>([]<typename Field>() {
    std::cout << Field::id()
      << " deflt=" << typename Field::Type::template Print<>{Field::deflt()}
      << MinMax<Field>{}
      << (Field::Type::Code == ZtFieldTypeCode::Bytes ? "" : "\n");
  });
  std::cout << '\n';
  ZtFieldVFmt fmt;
  ZtVFieldArray fields{ZtVFields<Foo>()};
  auto print = [&fmt](auto &s, const ZtVField &field, int constant) {
    using namespace ZtFieldTypeCode;
    ZuSwitch::dispatch<ZtFieldTypeCode::N>(field.type->code,
	[&s, constant, &field, &fmt](auto Code) {
      field.constant.print<Code>(s, ZtVField::cget(constant), &field, fmt);
    });
  };
  for (unsigned i = 0, n = fields.length(); i < n; i++) {
    std::cout << fields[i]->id;
    auto type = fields[i]->type;
    if (type->code == ZtFieldTypeCode::UDT) {
      std::cout << " udt=" << ZmDemangle{type->info.udt()->info->name()};
    } else if (type->props & ZtVFieldProp::Enum()) {
      std::cout << " enum=" << type->info.enum_()->id();
    } else if (type->props & ZtVFieldProp::Flags()) {
      std::cout << " flags=" << type->info.flags()->id();
    }
    std::cout << " deflt=";
    print(std::cout, *fields[i], ZtVFieldConstant::Deflt);
    using namespace ZtFieldTypeCode;
    switch (type->code) {
      case Int32:
      case UInt32:
      case Float:
      case Fixed:
      case Decimal:
	std::cout << " minimum=";
	print(std::cout, *fields[i], ZtVFieldConstant::Minimum);
	std::cout << " maximum=";
	print(std::cout, *fields[i], ZtVFieldConstant::Maximum);
	break;
      case Bytes:
	continue;
    }
    std::cout << '\n';
  }
  {
    Foo foo;
    foo.bytesVec = { "xxx", "yyyy", "zzzzz" };
    std::cout << foo << '\n';
  }
}
