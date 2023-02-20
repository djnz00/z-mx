#include <stdlib.h>
#ifndef _WIN32
#include <alloca.h>
#endif

#include <iostream>
#include <string>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuByteSwap.hpp>

#include <zlib/Zfb.hpp>
#include <zlib/ZfbTree.hpp>

#include <zlib/tree_fbs.h>

using IOBuilder = Zfb::IOBuilder<>;
using IOBuf = IOBuilder::IOBuf;
std::vector<ZmRef<IOBuf>> bufs;

template <bool Detach>
void build(IOBuilder &fbb, unsigned n)
{
  using namespace ZfbTree;
  ZmRef<IOBuf> buf;
  {
    using namespace ZfbTree::Save;
    uint8_t *zero = reinterpret_cast<uint8_t *>(::malloc(n));
    memset(zero, 0, n);
    fbb.Clear();
    auto o = tree(fbb, vector<fbs::Item>(fbb,
	  itemNested(fbb, "key1", vector<fbs::Item>(fbb,
	      itemString(fbb, "nested_key1", "nested_value"))),
	  itemNested(fbb, "key2", vector<fbs::Item>(fbb,
	      itemNested_(fbb, "nested_key2", bytes(fbb, zero, n))))));
    fbb.Finish(o);
    fbb.PushElement(static_cast<uint32_t>(42));
    fbb.PushElement(static_cast<uint32_t>(fbb.GetSize()));
    ::free(zero);
    if constexpr (Detach) { buf = fbb.buf(); bufs.push_back(buf); }
  }
  {
    using namespace Load;

    uint8_t *ptr = Detach ? buf->data() : fbb.GetBufferPointer();
    int len = Detach ? buf->length : fbb.GetSize();
    uint32_t len_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr);
    uint32_t type_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr + 4);
    auto tree = fbs::GetTree(ptr + 8);
    auto item = tree->items()->Get(1);
    auto key = str(item->key());
    auto nested = item->value_as_Nested();
    auto data = bytes(nested->value());
    std::cout
      << "ptr=" << ZuBoxPtr(ptr).hex() << " len=" << len
      << " len_=" << len_ << " type_=" << type_
      << " key2=" << key
      << " value_type=" << fbs::EnumNameValue(item->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    tree = nested->value_nested_root();
    item = tree->items()->Get(0);
    key = str(item->key());
    nested = item->value_as_Nested();
    data = bytes(nested->value());
    std::cout
      << "nested tree ptr=" << ZuBoxPtr(tree).hex()
      << " key2=" << key
      << " value_type=" << fbs::EnumNameValue(item->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    tree = fbs::GetTree(ptr + 8);
    item = tree->items()->Get(0);
    key = str(item->key());
    nested = item->value_as_Nested();
    data = bytes(nested->value());
    std::cout
      << "key1=" << key
      << " value_type=" << fbs::EnumNameValue(item->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    tree = nested->value_nested_root();
    item = tree->items()->Get(0);
    key = str(item->key());
    auto string = item->value_as_String();
    std::cout
      << "nested tree ptr=" << ZuBoxPtr(tree).hex()
      << " key=" << key
      << " value_type=" << fbs::EnumNameValue(item->value_type())
      << " value=" << str(string) << '\n' << std::flush;
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
