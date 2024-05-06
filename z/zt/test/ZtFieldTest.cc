//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZuID.hh>

#include <zlib/ZmDemangle.hh>

#include <zlib/ZtField.hh>

namespace Values {
  ZtEnumValues(Values, High, Low, Normal);
}

namespace Flags {
  ZtEnumFlags(Flags, Bit0, Bit1, Bit2);
}

struct Nested {
  int i1, i2;

  friend ZtFieldPrint ZuPrintType(Nested *);
};

ZtFields(Nested, (((i1)), (Int), (Ctor<0>)), (((i2)), (Int), (Ctor<1>)));

struct Foo {
  const char *string = nullptr;
  ZtArray<uint8_t> bytes;
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
  Nested nested;

  friend ZtFieldPrint ZuPrintType(Foo *);
};

ZtFields(Foo,
    (((string, Rd)), (CString, "hello \"world\""), (Ctor<0>, Quote)),
    (((bytes)), (Bytes, ZuBytes{"bytes"}), (Ctor<1>)),
    (((id)), (String, "goodbye"), (Ctor<2>)),
    (((int_)), (Int), (Ctor<3>)),
    (((int_ranged)), (Int, 42, 0, 100), (Ctor<4>)),
    (((hex)), (UInt, 0xdeadbeef), (Ctor<5>, Hex)),
    (((enum_)), (Enum, Values::Map, Values::Normal), (Ctor<6>)),
    (((flags)), (Flags, Flags::Map, Flags::Bit1), (Ctor<7>)),
    (((float_)), (Float), (Ctor<8>)),
    (((float_ranged)), (Float, 0.42, 0.0, 1), (Ctor<9>)),
    (((fixed)), (Fixed), (Ctor<10>)),
    (((decimal)), (Decimal), (Ctor<11>)),
    (((time_)), (Time), (Ctor<12>)),
    (((nested)), (UDT), (Ctor<13>)));

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
  ZuUnroll::all<Fields>([&fmt]<typename Field>() {
    std::cout << Field::id()
      << " deflt=" << typename Field::Type::Print{Field::deflt(), fmt}
      << MinMax<Field>{fmt}
      << (Field::Type::Code == ZtFieldTypeCode::Bytes ? "" : "\n");
  });
  std::cout << '\n';
  ZtMFields fields{ZtMFieldList<Foo>()};
  auto print = [&fmt](auto &s, const ZtMField &field, int constant) {
    using namespace ZtFieldTypeCode;
    using ZtFieldTypeCode::Flags;
    ZuSwitch::dispatch<ZtFieldTypeCode::N>(field.type->code,
	[&s, constant, &field, &fmt](auto Code) {
      field.constant.print<Code>(s, ZtMField::cget(constant), 0, &field, fmt);
    });
  };
  for (unsigned i = 0, n = fields.length(); i < n; i++) {
    std::cout << fields[i]->id;
    auto type = fields[i]->type;
    using namespace ZtFieldTypeCode;
    using ZtFieldTypeCode::Flags;
    switch (type->code) {
      case Enum:
	std::cout << " enum=" << type->info.enum_()->id();
	break;
      case Flags:
	std::cout << " flags=" << type->info.flags()->id();
	break;
      case UDT:
	std::cout << " udt=" << ZmDemangle{type->info.udt()->info->name()};
	break;
    }
    std::cout << " deflt=";
    print(std::cout, *fields[i], ZtMFieldConstant::Deflt);
    switch (type->code) {
      case Int:
      case UInt:
      case Float:
      case Fixed:
      case Decimal:
	std::cout << " minimum=";
	print(std::cout, *fields[i], ZtMFieldConstant::Minimum);
	std::cout << " maximum=";
	print(std::cout, *fields[i], ZtMFieldConstant::Maximum);
	break;
      case Bytes:
	continue;
    }
    std::cout << '\n';
  }

  auto i = ZtField::importer<Foo>();
  auto x = ZtField::exporter<Foo>();
  Foo data{
    .string = "foo bar",
    .bytes = "yikes",
    .id = "hello",
    .int_ = -42,
    .int_ranged = 42,
    .hex = 43,
    .enum_ = -1,
    .flags = 0,
    .float_ = -0.42,
    .float_ranged = 0.42,
    .fixed = { 0.42, 2 },
    .decimal = 0.42,
    .time_ = ZmTimeNow(),
    .nested = { 42, 43 }
  };
  std::cout << '\n' << ZtField::ctor<Foo>(ZtField::Import{i, &data}) << '\n';
  Foo data2;
  ZtField::Export x_{x, &data2};
  ZtField::save<Foo>(data, x_);
  std::cout << '\n' << data2 << '\n';
}
