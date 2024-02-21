//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuID.hpp>

#include <zlib/ZtField.hpp>

namespace Values {
  ZtEnumValues("Values", High, Low, Normal);
}

namespace Flags {
  ZtEnumValues_(Bit0, Bit1, Bit2);
  ZtEnumFlags("Flags", Map, "Bit0", Bit0, "Bit1", Bit1, "Bit2", Bit2);
}

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
};

ZtFields(Foo,
    (((string, Rd)), (String, "hello \"world\""), (Ctor<0>, Quote)),
    (((bytes)), (Bytes, ZuBytes{"bytes"}), (Ctor<1>)),
    (((id)), (String, "goodbye"), (Ctor<2>)),
    (((int_)), (Int), (Ctor<3>)),
    (((int_ranged)), (Int, 42, 0, 100), (Ctor<4>)),
    (((hex)), (Hex, 0xdeadbeef), (Ctor<5>)),
    (((enum_)), (Enum, Values::Map, Values::Normal), (Ctor<6>)),
    (((flags)), (Flags, Flags::Map, (1<<Flags::Bit1)), (Ctor<7>)),
    (((float_)), (Float), (Ctor<8>)),
    (((float_ranged)), (Float, 0.42, 0.0, 1), (Ctor<9>)),
    (((fixed)), (Fixed), (Ctor<10>)),
    (((decimal)), (Decimal), (Ctor<11>)),
    (((time_)), (Time), (Ctor<12>)));

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
      << MinMax<Field>{fmt}
      << (Field::Type == ZtFieldType::Bytes ? "" : "\n");
  });
  ZtVFieldArray fields{ZtVFields<Foo>()};
  auto print = [&fmt](auto &s, const ZtVField &field, int constant) {
    using namespace ZtFieldType;
    using ZtFieldType::Flags;
    const auto &fn = field.constantFn;
    switch (field.type) {
      case String:	s << fn.string(constant); break;
      case Bytes:	s << ZtHexDump_{fn.bytes(constant)}; break;
      case Composite: {
	ZmStream s_{s};
	fn.composite(constant)(s_);
      } break;
      case Bool:
      case Int:		s << fn.int_(constant); break;
      case Hex:		s << ZuBoxed(fn.int_(constant)).hex(); break;
      case Enum:	s << field.info.enum_()->v2s(fn.int_(constant)); break;
      case Flags: {
	ZmStream s_{s};
	field.info.flags()->print(fn.int_(constant), s_, fmt);
      } break;
      case Float:	s << fn.float_(constant); break;
      case Fixed:	s << fn.fixed(constant); break;
      case Decimal:	s << fn.decimal(constant); break;
      case Time:	s << fn.time(constant); break;
    }
  };
  for (unsigned i = 0, n = fields.length(); i < n; i++) {
    std::cout << fields[i]->id << " deflt=";
    print(std::cout, *fields[i], ZtVFieldConstant::Deflt);
    if (fields[i]->type == ZtFieldType::Bytes) continue;
    std::cout << " minimum=";
    print(std::cout, *fields[i], ZtVFieldConstant::Minimum);
    std::cout << " maximum=";
    print(std::cout, *fields[i], ZtVFieldConstant::Maximum);
    std::cout << '\n';
  }
}
