//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stddef.h>

#include <zlib/ZmList.hpp>

#include <zlib/ZtEnum.hpp>

#include <zlib/ZiFile.hpp>

#include <zlib/ZvCSV.hpp>

// 2011/11/11 12:00:00
// 2011/11/11 12:12:12
static const char *testdata =
  "string,int,bool,float,enum,time,flags,func,A,B,C,,\n"
  "string,199,Y,1.234,sasha,2011/11/11 12:12:12,Flag1,A,B,C,D,,,\n"
  "string2,23,N,0.00042,grey,2011/11/11 12:12:12.1234,SUP,,,\n"
  "\"-,>\"\"<,-\",2,,0.0000002,\"\"\"\"girlfriend,,Flag1|Flag2,,,\n"
  "-->\",\"<--,3,N,3.1415926,\"experience\"\"\",,Flag1,,,\n";

namespace Enums {
  ZtEnumValues_(Sasha = 1, Grey, Girlfriend = 43, Experience, __);
  ZtEnumMap("Enums", Map,
      "sasha", 1, "grey", 42, "\"girlfriend", 43, "experience\"", 44, "", 45);
}

namespace DaFlags {
  ZtEnumValues_(Flag1, Flag2, P, SUP);
  ZtEnumFlags("DaFlags", Map, "Flag1", 0, "Flag2", 1, "P", 2, "SUP", 3);
}

struct Row {
  ZuStringN<24>	m_string;
  int		m_int;
  int		m_bool;
  double	m_float;
  int		m_enum;
  ZtDate	m_time;
  int		m_flags;
};
ZtFields(Row,
    (((string, Alias, m_string)), (String), (Ctor(0))),
    (((int, Alias, m_int)), (Int), (Ctor(1))),
    (((bool, Alias, m_bool)), (Bool), (Ctor(2))),
    (((float, Alias, m_float)), (Float), (Ctor(3), NDP(2))),
    (((enum, Alias, m_enum)), (Enum, Enums::Map), (Ctor(4))),
    (((time, Alias, m_time)), (Time), (Ctor(5))),
    (((flags, Alias, m_flags)), (Flags, DaFlags::Map), (Ctor(6))));

using CSVWrite = ZmList<ZuRef<ZuPOD<Row> > >;

struct RowSet {
  Row *alloc() {
    ZuRef<ZuPOD<Row> > row = new ZuPOD<Row>();
    new (row->ptr()) Row();
    rows.push(row);
    return row->ptr();
  }
  ZmList<ZuRef<ZuPOD<Row> > >	rows;
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

      ZuRef<ZuPOD<Row> > row;

      CSVWrite unFiltList;
      CSVWrite filtList;

      while (row = rowSet.rows.shift()) {
	printf("%s, %d, %c, %f, %s (%d:%d) %i\n", 
	       row->ptr()->m_string.data(),
	       (int)row->ptr()->m_int,
	       row->ptr()->m_bool ? 'Y' : 'N',
	       (double)row->ptr()->m_float,
	       (const char *)Enums::Map::v2s(row->ptr()->m_enum),
	       (row->ptr()->m_time).yyyymmdd(),
	       (row->ptr()->m_time).hhmmss(),
	       (int)row->ptr()->m_flags);
	unFiltList.push(row);
	filtList.push(row);
      }

      ZtArray<ZtString> filter;
      filter.push("*");
      {
	auto fn = csv.writeFile("all.written.csv", filter);
	while (ZuRef<ZuPOD<Row>> pod = unFiltList.shift())
	  fn(pod->ptr());
	fn(nullptr);
      }
      filter.null();
      filter.push("string");
      filter.push("flags");
      {
	auto fn = csv.writeFile("filt.written.csv", filter);
	while (ZuRef<ZuPOD<Row>> pod = filtList.shift())
	  fn(pod->ptr());
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
