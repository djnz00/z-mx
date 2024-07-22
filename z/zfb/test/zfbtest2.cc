//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>
#include <string>

#include <zlib/ZuBox.hh>
#include <zlib/ZuByteSwap.hh>

#include <zlib/ZtHexDump.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include "zfbtest2_fbs.h"

namespace zfbtest2 {

using namespace Zfb;

struct Object {
  ZuID id;
  int price;
  ZuBitmap<100> flags;

  friend ZtFieldPrint ZuPrintType(Object *);
};
struct Test {
  int foo = 42;
  ZtString bar;
  Object baz;

  friend ZtFieldPrint ZuPrintType(Test *);
};

ZfbFieldTbl(Object,
  (((id), (Ctor<0>)), (ID)),
  (((price), (Ctor<1>)), (Int32)),
  (((flags), (Ctor<2>)), (Bitmap, ZuBitmap<100>{"42"})));
ZfbFieldTbl(Test,
  (((foo), (Ctor<0>)), (Int32)),
  (((bar), (Ctor<1>)), (String)),
  (((baz), (Ctor<2>)), (Object)));
ZfbRoot(Test);

static auto mfields = ZtVFields<Test>();

} // zfbtest

using IOBuilder = Zfb::IOBuilder;
using IOBuf = ZiIOBuf;
std::vector<ZmRef<IOBuf>> bufs;

template <bool Detach>
void build(IOBuilder &fbb, unsigned n)
{
  using namespace Zfb;
  ZmRef<IOBuf> buf;
  {
    zfbtest2::Test test{42, "Hello", {"id", 142}};
    fbb.Clear();
    fbb.Finish(ZfbField::save(fbb, test));
    fbb.PushElement(static_cast<uint32_t>(fbb.GetSize()));
    if constexpr (Detach) { buf = fbb.buf(); bufs.push_back(buf); }
  }
  {
    uint8_t *ptr = Detach ? buf->data() : fbb.GetBufferPointer();
    int len = Detach ? buf->length : fbb.GetSize();

    uint32_t len_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr);
    std::cout << ZtHexDump(ZtString{} << ZuBoxPtr(ptr).hex(), ptr, len);
    auto test = zfbtest2::fbs::GetTest(ptr + 4);
    std::cout
      << "ptr=" << ZuBoxPtr(ptr).hex() << " len=" << len
      << " len_=" << len_
      << '\n' << std::flush;
    std::cout << ZfbField::Load<zfbtest2::Test>{test} << '\n' << std::flush;
  }
}

int main(int argc, char **argv)
{
  if (argc != 2) { std::cerr << "usage N\n" << std::flush; exit(1); }
  unsigned n = ZuBox<unsigned>(argv[1]);
  IOBuilder fbb;
  build<false>(fbb, n);
  build<true>(fbb, n);
  build<true>(fbb, n);
  build<false>(fbb, n);
  build<true>(fbb, n);
  build<true>(fbb, n);

  for (auto field : zfbtest2::mfields)
    std::cout << *field << '\n';
}
