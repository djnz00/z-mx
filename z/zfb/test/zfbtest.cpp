#include <stdlib.h>
#ifndef _WIN32
#include <alloca.h>
#endif

#include <iostream>
#include <string>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuByteSwap.hpp>

#include <zlib/Zfb.hpp>

#include "zfbtest_fbs.h"

using IOBuilder = Zfb::IOBuilder<>;
using IOBuf = IOBuilder::IOBuf;
std::vector<ZmRef<IOBuf>> bufs;

template <bool Detach>
void build(IOBuilder &fbb, unsigned n)
{
  ZmRef<IOBuf> buf;
  {
    uint8_t *zero = reinterpret_cast<uint8_t *>(::malloc(n));
    memset(zero, 0, n);
    fbb.Clear();
    auto o = zfbtest::fbs::CreateBuf(fbb, Zfb::Save::bytes(fbb, zero, n));
    fbb.Finish(o);
    fbb.PushElement(static_cast<uint32_t>(42));
    fbb.PushElement(static_cast<uint32_t>(fbb.GetSize()));
    ::free(zero);
    if constexpr (Detach) { buf = fbb.buf(); bufs.push_back(buf); }
  }
  {
    uint8_t *ptr = Detach ? buf->data() : fbb.GetBufferPointer();
    int len = Detach ? buf->length : fbb.GetSize();
    uint32_t len_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr);
    uint32_t type_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr + 4);
    auto buf_ = zfbtest::fbs::GetBuf(ptr + 8);
    auto data = Zfb::Load::bytes(buf_->data());
    std::cout
      << "ptr=" << ZuBoxPtr(ptr).hex() << " len=" << len
      << " len_=" << len_ << " type_=" << type_
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
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
}
