//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuID.hpp>

#include <zlib/ZmDemangle.hpp>

#include <zlib/ZtField.hpp>

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
  const char *string;
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
    (((string, Rd)), (String, "hello \"world\""), (Ctor<0>, Quote)),
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
  ZuTypeAll<Fields>::invoke([&fmt]<typename Field>() {
    std::cout << Field::id()
      << " deflt=" << typename Field::Type::Print{Field::deflt(), fmt}
      << MinMax<Field>{fmt}
      << (Field::Type::Code == ZtFieldTypeCode::Bytes ? "" : "\n");
  });
  std::cout << '\n';
  ZtVFieldArray fields{ZtVFields<Foo>()};
  auto print = [&fmt](auto &s, const ZtVField &field, int constant) {
    using namespace ZtFieldTypeCode;
    using ZtFieldTypeCode::Flags;
    const void *ptr = field.constant(constant);
    ZmStream s_{s};
    field.type->print(ptr, s_, fmt);
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
	std::cout << " udt=" << ZmDemangle{type->info.udt->name()};
	break;
    }
    std::cout << " deflt=";
    print(std::cout, *fields[i], ZtVFieldConstant::Deflt);
    switch (type->code) {
      case Int:
      case UInt:
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

  ZtField::Importer i{ {
    ZtVFieldGet{.fn = {.string = [](const void *) -> ZuString { return "foo bar"; }}},
    ZtVFieldGet{.fn = {.bytes = [](const void *) -> ZuBytes { return {"yikes"}; }}},
    ZtVFieldGet{.fn = {.string = [](const void *) -> ZuString { return "hello"; }}},
    ZtVFieldGet{.fn = {.int_ = [](const void *) -> int64_t { return -42; }}},
    ZtVFieldGet{.fn = {.int_ = [](const void *) -> int64_t { return 42; }}},
    ZtVFieldGet{.fn = {.uint = [](const void *) -> uint64_t { return 43; }}},
    ZtVFieldGet{.fn = {.enum_ = [](const void *) -> int { return -1; }}},
    ZtVFieldGet{.fn = {.flags = [](const void *) -> uint64_t { return 0; }}},
    ZtVFieldGet{.fn = {.float_ = [](const void *) -> double { return -0.42; }}},
    ZtVFieldGet{.fn = {.float_ = [](const void *) -> double { return 0.42; }}},
    ZtVFieldGet{.fn = {.fixed = [](const void *) -> ZuFixed { return {0.42, 2}; }}},
    ZtVFieldGet{.fn = {.decimal = [](const void *) -> ZuDecimal { return 0.42; }}},
    ZtVFieldGet{.fn = {.time = [](const void *) -> ZmTime { return ZmTimeNow(); }}},
    ZtVFieldGet{.fn = {.udt = [](const void *) -> const void * {
      static Nested n{42, 43};
      return &n;
    }}}
  } };
  ZtField::Import j{i};
  std::cout << ZtField::ctor<Foo>(j) << '\n';
}
