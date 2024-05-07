//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// CSV parser/generator

// Microsoft Excel compatible quoting: a, " ,"",",b -> a| ,",|b
// Unlike Excel, leading white space is discarded if not quoted

#ifndef ZvCSV_HH
#define ZvCSV_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <stdio.h>

#include <zlib/ZuBox.hh>
#include <zlib/ZuString.hh>

#include <zlib/ZuRef.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmFn.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtDate.hh>
#include <zlib/ZtString.hh>
#include <zlib/ZtRegex.hh>

#include <zlib/ZePlatform.hh>

#include <zlib/ZvError.hh>
#include <zlib/ZtField.hh>
#include <zlib/ZvCSV.hh>

#define ZvCSV_MaxLineSize	(8<<10)	// 8K

namespace ZvCSV_ {
  ZvExtern void split(ZuString row, ZtArray<ZtArray<char>> &a);

  // CSV string quoting
  template <typename Row>
  inline void quote__(Row &row, ZuString s) {
    row << '"';
    for (unsigned i = 0, n = s.length(); i < n; i++) {
      char c = s[i];
      row << c;
      if (ZuUnlikely(c == '"')) row << '"'; // double-up quotes within quotes
    }
    row << '"';
  }
  // use built-in printing as-is
  template <unsigned Code, typename Row, typename T>
  inline
  ZuIfT<Code != ZtFieldTypeCode::CString &&
	Code != ZtFieldTypeCode::String &&
	Code != ZtFieldTypeCode::UDT &&
	Code != ZtFieldTypeCode::Enum &&
	Code != ZtFieldTypeCode::Flags>
  quote_(
      Row &row, const T *object, unsigned i,
      const ZtMField *field, const ZtFieldFmt &fmt) {
    ZmStream s{row};
    field->get.print<Code>(s, object, i, field, fmt);
  }
  // get strings without quoting, then quote for CSV
  template <unsigned Code, typename Row, typename T>
  inline
  ZuIfT<Code == ZtFieldTypeCode::CString ||
	Code == ZtFieldTypeCode::String>
  quote_(
      Row &row, const T *object, unsigned i,
      const ZtMField *field, const ZtFieldFmt &fmt) {
    quote__(row, field->get.get<Code>(object, i));
  }
  // use the built-in print function, but quote for CSV
  template <unsigned Code, typename Row, typename T>
  inline
  ZuIfT<Code == ZtFieldTypeCode::UDT ||
	Code == ZtFieldTypeCode::Enum ||
	Code == ZtFieldTypeCode::Flags>
  quote_(
      Row &row, const T *object, unsigned i,
      const ZtMField *field, const ZtFieldFmt &fmt) {
    ZtString s;
    field->get.print<Code>(s, object, i, field, fmt);
    quote__(row, ZuMv(s));
  }
  // entry point for quoting values to be written
  template <typename Row, typename T>
  inline void quote(Row &row,
      const T *object, unsigned i,
      const ZtMField *field,
      const ZtFieldFmt &fmt) {
    ZuSwitch::dispatch<ZtFieldTypeCode::N>(field->type->code,
	[&row, object, i, field, &fmt](auto Code) {
      quote_<Code>(row, object, i, field, fmt);
    });
  }
}

class ZvAPI ZvCSV_FileIOError : public ZvError {
public:
  template <typename FileName>
  ZvCSV_FileIOError(const FileName &fileName, ZeError e) :
    m_fileName(fileName), m_error(e) { }

  void print_(ZmStream &s) const {
    s << "\"" << m_fileName << "\" " << m_error;
  }

private:
  ZtString	m_fileName;
  ZeError	m_error;
};

template <typename T> class ZvCSV {
  ZvCSV(const ZvCSV &) = delete;
  ZvCSV &operator =(const ZvCSV &) = delete;

public:
  using Fields = ZtMFields;
  using Field = ZtMField;
  using Column = ZuTuple<int, const Field *>;
  using ColNames = ZtArray<ZuString>;
  using ColIndex = ZtArray<int>;
  using ColArray = ZtArray<Column>;

private:
  static const char *ColTree_HeapID() { return "ZvCSV.ColTree"; }
  using ColTree =
    ZmRBTreeKV<ZuString, Column,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ColTree_HeapID>>>;

public:
  ZvCSV() {
    m_fields = ZtMFieldList<T>();
    for (int i = 0, n = m_fields.length(); i < n; i++)
      m_columns.add(m_fields[i]->id, Column{i, m_fields[i]});
  }

  struct FieldFmt : public ZtFieldFmt {
    FieldFmt() {
      new (dateScan.new_csv()) ZtDateScan::CSV{};
      new (datePrint.new_csv()) ZtDateFmt::CSV{};
    }
  };
  static FieldFmt &fmt() { return ZmTLS<FieldFmt, fmt>(); }

  Column find(ZuString id) const {
    auto node = m_columns.find(id);
    if (!node) return {-1, nullptr};
    return node->val();
  }

  const Field *field(unsigned i) const {
    if (i >= m_fields.length()) return nullptr;
    return m_fields[i];
  }

private:
  void writeHeaders(ZtArray<ZuString> &headers) const {
    unsigned n = m_fields.length();
    headers.size(n);
    for (unsigned i = 0; i < n; i++) headers.push(m_fields[i]->id);
  }

  void header(ColIndex &colIndex, ZuString hdr) const {
    ZtArray<ZtArray<char>> a;
    ZvCSV_::split(hdr, a);
    unsigned n = m_fields.length();
    colIndex.null();
    colIndex.length(n);
    for (unsigned i = 0; i < n; i++) colIndex[i] = -1;
    n = a.length();
    for (unsigned i = 0; i < n; i++) {
      int j = find(a[i]).template p<0>();
      if (j >= 0) colIndex[j] = i;
    }
  }

  void scan(
      const ColIndex &colIndex, ZuString row,
      const ZtFieldFmt &fmt, T *object) const {
    ZtArray<ZtArray<char>> a;
    ZvCSV_::split(row, a);
    unsigned n = a.length();
    unsigned m = colIndex.length();
    for (unsigned i = 0; i < m; i++) {
      int j;
      if ((j = colIndex[i]) < 0 || j >= static_cast<int>(n)) {
	auto field = m_fields[i];
	ZuSwitch::dispatch<ZtFieldTypeCode::N>(field->type->code,
	    [object, i, field, &fmt](auto Code) {
	  field->set.scan<Code>(object, i, ZuString{}, field, fmt);
	});
      }
    }
    for (unsigned i = 0; i < m; i++) {
      int j;
      if ((j = colIndex[i]) >= 0 && j < static_cast<int>(n)) {
	ZuString s = a[j];
	auto field = m_fields[i];
	ZuSwitch::dispatch<ZtFieldTypeCode::N>(field->type->code,
	    [object, i, &s, field, &fmt](auto Code) {
	  field->set.scan<Code>(object, i, s, field, fmt);
	});
      }
    }
  }

  ColArray columns(const ColNames &names) const {
    ColArray colArray;
    if (!names.length() || (names.length() == 1 && names[0] == "*")) {
      unsigned n = m_fields.length();
      colArray.size(n);
      for (unsigned i = 0; i < n; i++)
	colArray.push(ZuFwdTuple(static_cast<int>(i), m_fields[i]));
    } else {
      unsigned n = names.length();
      colArray.size(n);
      for (unsigned i = 0; i < n; i++)
	if (auto field = find(names[i]))
	  colArray.push(ZuMv(field));
    }
    return colArray;
  }

public:
  using FileIOError = ZvCSV_FileIOError;

  template <typename Alloc, typename Read>
  void readFile(const char *fileName, Alloc alloc, Read read) const {
    FILE *file;

    if (!(file = fopen(fileName, "r")))
      throw FileIOError(fileName, ZeLastError);

    ColIndex colIndex;
    ZtString row(ZvCSV_MaxLineSize);
    const auto &fmt = this->fmt();

    if (char *data = fgets(row.data(), ZvCSV_MaxLineSize - 1, file)) {
      row.calcLength();
      row.chomp();
      header(colIndex, row);
      row.length(0);
      while (data = fgets(row.data(), ZvCSV_MaxLineSize - 1, file)) {
	row.calcLength();
	row.chomp();
	if (row.length()) {
	  auto object = alloc();
	  if (!object) break;
	  scan(colIndex, row, fmt, object);
	  read(ZuMv(object));
	  row.length(0);
	}
      }
    }

    fclose(file);
  }

  template <typename Alloc, typename Read>
  void readData(ZuString data, Alloc alloc, Read read) const {
    ColIndex colIndex;
    const auto &fmt = this->fmt();

    ZtArray<ZtArray<char>> rows;
    if (!ZtREGEX("\n").split(data, rows)) return;
    ZtString row{ZvCSV_MaxLineSize};
    row.init(rows[0].data(), rows[0].length());
    row.chomp();
    header(colIndex, row);
    row.length(0);
    for (unsigned i = 1; i < rows.length(); i++) {
      row.init(rows[i].data(), rows[i].length());
      row.chomp();
      if (row.length()) {
	auto object = alloc();
	if (!object) break;
	scan(colIndex, row, fmt, object);
	read(ZuMv(object));
	row.length(0);
      }
    }
  }

  auto writeFile(const char *fileName, const ColNames &columns) const {
    FILE *file;

    if (!strcmp(fileName, "&1"))
      file = stdout;
    else if (!strcmp(fileName, "&2"))
      file = stderr;
    else if (!(file = fopen(fileName, "w")))
      throw FileIOError(fileName, ZeLastError);

    ColArray colArray = this->columns(columns);
    const auto &fmt = this->fmt();

    ZtString *row = new ZtString{ZvCSV_MaxLineSize};

    for (unsigned i = 0, n = colArray.length(); i < n; i++) {
      if (ZuLikely(i)) *row << ',';
      *row << colArray[i].p<1>()->id;
    }
    *row << '\n';
    fwrite(row->data(), 1, row->length(), file);

    return [colArray = ZuMv(colArray), row, &fmt, file](const T *object) {
      if (ZuUnlikely(!object)) {
	delete row;
	fclose(file);
	return;
      }
      row->length(0);
      for (unsigned i = 0, n = colArray.length(); i < n; i++) {
	if (ZuLikely(i)) *row << ',';
	ZvCSV_::quote(
	  *row, object, colArray[i].p<0>(), colArray[i].p<1>(), fmt);
      }
      *row << '\n';
      fwrite(row->data(), 1, row->length(), file);
    };
  }
  auto writeFile(const char *fileName) const {
    return writeFile(fileName, ColNames{});
  }
  auto writeData(ZtString &data, const ColNames &columns) const {
    ColArray colArray = this->columns(columns);
    const auto &fmt = this->fmt();

    ZtString *row = new ZtString{ZvCSV_MaxLineSize};

    for (unsigned i = 0, n = colArray.length(); i < n; i++) {
      if (ZuLikely(i)) *row << ',';
      *row << colArray[i].p<1>()->id;
    }
    *row << '\n';
    data << *row;

    return [colArray = ZuMv(colArray), row, &fmt, &data](const T *object) {
      if (ZuUnlikely(!object)) {
	delete row;
	return;
      }
      row->length(0);
      for (unsigned i = 0, n = colArray.length(); i < n; i++) {
	if (ZuLikely(i)) *row << ',';
	ZvCSV_::quote(
	  *row, object, colArray[i].p<0>(), colArray[i].p<1>(), fmt);
      }
      *row << '\n';
      data << *row;
    };
  }
  auto writeData(ZtString &data) const {
    return writeData(data, ColNames{});
  }

private:
  Fields	m_fields;
  ColTree	m_columns;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZvCSV_HH */
