//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// CSV parser/generator

// Microsoft Excel compatible quoting: a, " ,"",",b -> a| ,",|b
// Unlike Excel, leading white space is discarded if not quoted

#ifndef ZvCSV_HPP
#define ZvCSV_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <stdio.h>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuString.hpp>

#include <zlib/ZuRef.hpp>

#include <zlib/ZmObject.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmFn.hpp>

#include <zlib/ZtArray.hpp>
#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>
#include <zlib/ZtRegex.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZvError.hpp>
#include <zlib/ZtField.hpp>
#include <zlib/ZvCSV.hpp>

#define ZvCSV_MaxLineSize	(8<<10)	// 8K

namespace ZvCSV_ {
  ZvExtern void split(ZuString row, ZtArray<ZtArray<char>> &a);

  template <typename Row> void quote_(Row &row, ZuString s) {
    row << '"';
    for (unsigned i = 0, n = s.length(); i < n; i++) {
      char c = s[i];
      row << c;
      if (ZuUnlikely(c == '"')) row << '"'; // double-up quotes within quotes
    }
    row << '"';
  }
  template <typename T, typename Fmt, typename Row>
  void quote(Row &row, const ZtMField *field, const T *object, const Fmt &fmt) {
    switch (field->type->code) {
      case ZtFieldTypeCode::String: {
	quote_(row, field->get.fn<ZtFieldTypeCode::String>(object));
      } break;
      case ZtFieldTypeCode::UDT:
      case ZtFieldTypeCode::Enum:
      case ZtFieldTypeCode::Flags: {
	ZtString s_;
	ZmStream s{s_};
	field->type->print(object, s, fmt);
	quote_(row, s_);
      } break;
      default: {
	ZmStream s{row};
	field->type->print(object, s, fmt);
      } break;
    }
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
  using Fields = ZtMFieldArray;
  using Field = ZtMField;

  using ColNames = ZtArray<ZuString>;
  using ColIndex = ZtArray<int>;
  using ColArray = ZtArray<const Field *>;

private:
  static const char *ColTree_HeapID() { return "ZvCSV.ColTree"; }
  using ColTree =
    ZmRBTreeKV<ZuString, ZuPair<int, const Field *>,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ColTree_HeapID>>>;

public:
  ZvCSV() {
    m_fields = ZtMFields<T>();
    for (int i = 0, n = m_fields.length(); i < n; i++)
      m_columns.add(
	  m_fields[i]->id, ZuPair<int, const Field *>{i, m_fields[i]});
  }

  struct FieldFmt : public ZtFieldFmt {
    FieldFmt() {
      new (dateScan.init_csv()) ZtDateScan::CSV{};
      new (datePrint.init_csv()) ZtDateFmt::CSV{};
    }
  };
  FieldFmt &fmt() {
    thread_local FieldFmt fmt; // FIXME
    return fmt;
  }
  const FieldFmt &fmt() const {
    return const_cast<ZvCSV *>(this)->fmt();
  }

  ZuPair<int, const Field *> find(ZuString id) const {
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
      if ((j = colIndex[i]) < 0 || j >= (int)n)
        m_fields[i]->type->scan(object, ZuString{}, fmt);
    }
    for (unsigned i = 0; i < m; i++) {
      int j;
      if ((j = colIndex[i]) >= 0 && j < (int)n)
        m_fields[i]->type->scan(object, a[j], fmt);
    }
  }

  ColArray columns(const ColNames &names) const {
    ColArray colArray;
    if (!names.length() || (names.length() == 1 && names[0] == "*")) {
      unsigned n = m_fields.length();
      colArray.size(n);
      for (unsigned i = 0; i < n; i++) colArray.push(m_fields[i]);
    } else {
      unsigned n = names.length();
      colArray.size(n);
      for (unsigned i = 0; i < n; i++)
	if (auto field = find(names[i]).template p<1>())
	  colArray.push(field);
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
      *row << colArray[i]->id;
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
	ZvCSV_::quote(*row, colArray[i], object, fmt);
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
      *row << colArray[i]->id;
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
	ZvCSV_::quote(*row, colArray[i], object, fmt);
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

#endif /* ZvCSV_HPP */
