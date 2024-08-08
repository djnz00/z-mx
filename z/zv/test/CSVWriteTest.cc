//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stddef.h>

#include <zlib/ZmList.hh>

#include <zlib/ZtEnum.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvCSV.hh>

namespace Snafus {
  ZtEnumValues_(int8_t,
      Sasha = 1, Grey = 42, Girlfriend = 43, Experience = 44, TigerWoods = 45);
  ZtEnumMap(Snafus, Map,
      "sasha", 1, "grey", 42, "girlfriend", 43, "experience", 44,
      "tiger-woods", 45);
}

namespace DaFlags {
  ZtEnumFlags(DaFlags, uint32_t, S, A, P, SUP, HI);
}

struct Row {
  ZuStringN<24>	foo;
  bool		bar;
  int		bah;
  double	baz;
  ZuFixedVal	bam_;
  int		snafu;
  ZuDateTime	mabbit;
  int		flags;

  ZuFixed bam() const { return ZuFixed{bam_, 2}; }
  void bam(ZuFixed v) { bam_ = v.adjust(2); }
};
ZtFieldTbl(Row,
    (((foo), (Ctor<0>)), (String)),
    (((bar), (Ctor<1>)), (Bool)),
    (((bah), (Ctor<2>)), (Int32)),
    (((baz), (Ctor<3>, NDP<2>)), (Float)),
    (((bam, Fn), (Ctor<4>, NDP<2>)), (Fixed)),
    (((snafu), (Ctor<5>)), (Int32)),
    (((mabbit), (Ctor<6>)), (DateTime)),
    (((flags), (Ctor<7>, Flags<DaFlags::Map>)), (Int32)));

using CSVWrite = ZmList<Row, ZmListNode<ZuObject>>;

int main()
{
  try {
    ZvCSV<Row> csv;

    CSVWrite filtList;
    CSVWrite unFiltList;

    for (int i = 0; i < 10; i++) {
      auto node = new CSVWrite::Node{};
      Row *row = &node->val();
      row->foo = ZtSprintf("Sup Homie %d", i).data();
      row->bar = i % 2;
      row->bah = i * 2;
      row->baz = i * 2.2;
      row->bam_ = ZuFixed{row->baz * 2.2, 2}.mantissa;
      switch(i) {
	case 1: row->snafu = 1; break;
	case 2: row->snafu = 42; break;
	case 3: row->snafu = 43; break;
	case 4: row->snafu = 44; break;
	case 5: row->snafu = 45; break;
	default: row->snafu = 99; break;
      }
      row->mabbit = ZuDateTime{Zm::now()};
      switch(i) {
	case 1: row->flags = 0x10 | 0x08; break;
	case 2: row->flags = 0x01 | 0x02; break;
	case 3: row->flags = 0x04 | 0x08; break;
	case 4: row->flags = 0x10; break;
	case 5: row->flags = 0x08; break;
	default: row->flags = ZuCmp<int>::null(); break;
      }
      switch(i) {
	case 1: row->mabbit = ZuDateTime(2010, 01, 22, 15, 22, 14); break;
	default: break;
      }
      filtList.pushNode(node);
      unFiltList.pushNode(node);
    }

    ZtArray<ZtString> filter;
    filter.push("*");
    {
      auto fn = csv.writeFile("all.written.csv", filter);
      while (auto node = unFiltList.shift()) fn(&node->val());
      fn(nullptr);
    }
    filter.null();
    filter.push("foo");
    filter.push("flags");
    {
      auto fn = csv.writeFile("filt.written.csv", filter);
      while (auto node = filtList.shift()) fn(&node->val());
      fn(nullptr);
    }
  } catch (const ZvError &e) {
    std::cerr << "ZvError: " << e << '\n';
    Zm::exit(1);
  } catch (const ZeError &e) {
    std::cerr << "ZeError: " << e << '\n';
    Zm::exit(1);
  } catch (...) {
    std::cerr << "unknown exception\n";
    Zm::exit(1);
  }
  return 0;
}
