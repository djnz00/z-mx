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

// 2011/11/11 12:00:00
// 2011/11/11 12:12:12
static const char *testdata =
  "string,int,bool,float,enum,time,flags,func,A,B,C,,\n"
  "string,199,Y,1.234,sasha,2011/11/11 12:12:12,Flag1,A,B,C,D,,,\n"
  "string2,23,N,0.00042,grey,2011/11/11 12:12:12.1234,SUP,,,\n"
  "\"-,>\"\"<,-\",2,,0.0000002,\"\"\"\"girlfriend,,Flag1|Flag2,,,\n"
  "-->\",\"<--,3,N,3.1415926,\"experience\"\"\",,Flag1,,,\n";

namespace Enums {
  ZtEnumValues_(int8_t, Sasha = 1, Grey, Girlfriend = 43, Experience, __);
  ZtEnumMap(Enums, Map,
      "sasha", 1, "grey", 42, "\"girlfriend", 43, "experience\"", 44, "", 45);
}

namespace DaFlags {
  ZtEnumFlags(DaFlags, uint32_t, Flag1, Flag2, P, SUP);
}

struct Row {
  ZuStringN<24>	m_string;
  int		m_int;
  int		m_bool;
  double	m_float;
  int		m_enum;
  ZuDateTime	m_time;
  int		m_flags;
};
ZtFields(Row,
    (((string, Alias, m_string), (Ctor<0>)), (String)),
    (((int, Alias, m_int), (Ctor<1>)), (Int32)),
    (((bool, Alias, m_bool), (Ctor<2>)), (Bool)),
    (((float, Alias, m_float), (Ctor<3>, NDP<2>)), (Float)),
    (((enum, Alias, m_enum), (Ctor<4>, Enum<Enums::Map>)), (Int32)),
    (((time, Alias, m_time), (Ctor<5>)), (DateTime)),
    (((flags, Alias, m_flags), (Ctor<6>, Flags<DaFlags::Map>)), (UInt32)));

using CSVWrite = ZmList<Row>;

struct RowSet {
  using List = ZmList<Row>;
  using Node = List::Node;

  List		rows;

  Row *alloc() {
    Node *node = new Node{};
    rows.pushNode(node);
    return &node->data();
  }
};

int main()
{
  try {
    {
      ZiFile file;
      ZeError e;
      if (file.open("in.csv", ZiFile::Create | ZiFile::Truncate,
		    0777, &e) != Zi::OK)
	throw e;
      if (file.write((void *)testdata, (int)strlen(testdata), &e) != Zi::OK)
	throw e;
    }
    {
      ZvCSV<Row> csv;

      RowSet rowSet;
      csv.readFile("in.csv",
	  [&rowSet]() { return rowSet.alloc(); },
	  [](Row *) { });

      CSVWrite unFiltList;
      CSVWrite filtList;

      while (auto node = rowSet.rows.shift()) {
	const auto &row = node->val();
	std::cout << 
	  row.m_string.data() << ", " <<
	  row.m_int << ", " <<
	  (row.m_bool ? 'Y' : 'N') << ", " <<
	  row.m_float << ", " <<
	  Enums::Map::v2s(row.m_enum) << " (" <<
	  (row.m_time).yyyymmdd() << ':' <<
	  (row.m_time).hhmmss() << ") " <<
	  row.m_flags << '\n';
	unFiltList.push(row);
	filtList.push(row);
      }

      ZtArray<ZtString> filter;
      filter.push("*");
      {
	auto fn = csv.writeFile("all.written.csv", filter);
	while (auto node = unFiltList.shift()) fn(&node->val());
	fn(nullptr);
      }
      filter.null();
      filter.push("string");
      filter.push("flags");
      {
	auto fn = csv.writeFile("filt.written.csv", filter);
	while (auto node = filtList.shift()) fn(&node->val());
	fn(nullptr);
      }
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
