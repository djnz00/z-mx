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
    auto o1 = Zfb::Save::nest(fbb, [](Zfb::Builder &fbb) {
      return zfbtest::fbs::CreateTree(fbb,
	Zfb::Save::vector<zfbtest::fbs::Item>(fbb,
	  zfbtest::fbs::CreateItem(fbb,
	      Zfb::Save::str(fbb, "nested_key"),
	      zfbtest::fbs::Value_String,
	      zfbtest::fbs::CreateString(fbb,
		Zfb::Save::str(fbb, "nested_value")).Union())));
    });
    auto o2 = zfbtest::fbs::CreateTree(fbb,
	Zfb::Save::vector<zfbtest::fbs::Item>(fbb,
	  zfbtest::fbs::CreateItem(fbb,
	      Zfb::Save::str(fbb, "key1"),
	      zfbtest::fbs::Value_Nested,
	      zfbtest::fbs::CreateNested(fbb,
		Zfb::Save::bytes(fbb, zero, n)).Union()),
	  zfbtest::fbs::CreateItem(fbb,
	      Zfb::Save::str(fbb, "key2"),
	      zfbtest::fbs::Value_Nested,
	      zfbtest::fbs::CreateNested(fbb, o1).Union())));
    fbb.Finish(o2);
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
    auto tree = zfbtest::fbs::GetTree(ptr + 8);
    auto item = tree->items()->Get(0);
    auto key = Zfb::Load::str(item->key());
    auto nested = item->value_as_Nested();
    auto data = Zfb::Load::bytes(nested->value());
    std::cout
      << "ptr=" << ZuBoxPtr(ptr).hex() << " len=" << len
      << " len_=" << len_ << " type_=" << type_
      << " key1=" << key
      << " value_type=" << zfbtest::fbs::EnumNameValue(item->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    item = tree->items()->Get(1);
    key = Zfb::Load::str(item->key());
    nested = item->value_as_Nested();
    data = Zfb::Load::bytes(nested->value());
    std::cout
      << "key2=" << key
      << " value_type=" << zfbtest::fbs::EnumNameValue(item->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    tree = nested->value_nested_root();
    item = tree->items()->Get(0);
    key = Zfb::Load::str(item->key());
    auto string = item->value_as_String();
    std::cout
      << "nested tree ptr=" << ZuBoxPtr(tree).hex()
      << " value_type=" << zfbtest::fbs::EnumNameValue(item->value_type())
      << " value=" << Zfb::Load::str(string->value())
      << '\n' << std::flush;
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
