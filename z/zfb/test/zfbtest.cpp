//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <iostream>
#include <string>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuByteSwap.hpp>

#include <zlib/ZtHexDump.hpp>

#include <zlib/Zfb.hpp>
#include <zlib/ZfbKVTree.hpp>
#include <zlib/ZfbField.hpp>

#include "zfbtest_fbs.h"

namespace zfbtest {

using namespace Zfb;

struct Test {
  int foo = 42;
  ZtString bar;
  uint8_t *zero;
  unsigned n;

  friend ZtFieldPrint ZuPrintType(Test *);
};

ZfbFields(Test, fbs::Test,
  (((foo)), (Int), (Ctor<0>)),
  (((bar)), (String), (Ctor<1>)),
  (((kvTree, Lambda,
    ([](const Test &test) { return KVTreeGet{[&test]<typename B>(B &b) {
      using namespace Zfb::Save;
      return kvTree(b, vector<KV>(b,
	    kvNested(b, "key1", vector<KV>(b,
		kvString(b, "nested_key1", "nested_value"))),
	    kvNested(b, "key2", vector<KV>(b,
		kvUInt8Vec(b, "nested_key2",
		  bytes(b, test.zero, test.n))))));
    }, [&test]<typename S>(S &s) {
      s << "key1={nested_key1=nested_value} key2={nested_key2={"
	<< ZuBoxPtr(test.zero).hex() << "[" << test.n << "]}}";
    }}; }),
    ([](Test &test, const KVTree *kvTree) {
      using namespace Zfb::Load;
      auto data = bytes(kvTree->items()->Get(1)->
	  value_as_NestedKVTree()->data_nested_root()->items()->Get(0)->
	  value_as_UInt8Vec()->data());
      test.zero = const_cast<uint8_t *>(data.data());
      test.n = data.length();
    }))), (KVTree), (Synthetic)));

} // zfbtest

using IOBuilder = Zfb::IOBuilder<>;
using IOBuf = IOBuilder::IOBuf;
std::vector<ZmRef<IOBuf>> bufs;

template <bool Detach>
void build(IOBuilder &fbb, unsigned n)
{
  using namespace Zfb;
  ZmRef<IOBuf> buf;
  {
    zfbtest::Test test{42, "Hello"};
    test.zero = reinterpret_cast<uint8_t *>(::malloc(n));
    test.n = n;
    memset(test.zero, 0, test.n);
    fbb.Clear();
    fbb.Finish(ZfbField::save(fbb, test));
    fbb.PushElement(static_cast<uint32_t>(42));
    fbb.PushElement(static_cast<uint32_t>(fbb.GetSize()));
    ::free(test.zero);
    if constexpr (Detach) { buf = fbb.buf(); bufs.push_back(buf); }
  }
  {
    using namespace Load;

    uint8_t *ptr = Detach ? buf->data() : fbb.GetBufferPointer();
    int len = Detach ? buf->length : fbb.GetSize();
    uint32_t len_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr);
    uint32_t type_ = *reinterpret_cast<ZuLittleEndian<uint32_t> *>(ptr + 4);
    std::cout << ZtHexDump(ZtString{} << ZuBoxPtr(ptr).hex(), ptr, len);
    auto test = zfbtest::fbs::GetTest(ptr + 8);
    auto kvTree = test->kvTree();
    auto kv = kvTree->items()->Get(1);
    auto key = str(kv->key());
    auto nested = kv->value_as_NestedKVTree();
    auto data = bytes(nested->data());
    std::cout
      << "ptr=" << ZuBoxPtr(ptr).hex() << " len=" << len
      << " len_=" << len_ << " type_=" << type_
      << " key2=" << key
      << " value_type=" << EnumNameValue(kv->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    kvTree = nested->data_nested_root();
    kv = kvTree->items()->Get(0);
    key = str(kv->key());
    auto blob = kv->value_as_UInt8Vec();
    data = bytes(blob->data());
    std::cout
      << "nested kvTree ptr=" << ZuBoxPtr(kvTree).hex()
      << " key2=" << key
      << " value_type=" << EnumNameValue(kv->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    kvTree = test->kvTree();
    kv = kvTree->items()->Get(0);
    key = str(kv->key());
    nested = kv->value_as_NestedKVTree();
    data = bytes(nested->data());
    std::cout
      << "key1=" << key
      << " value_type=" << EnumNameValue(kv->value_type())
      << " data=" << ZuBoxPtr(data.data()).hex()
      << " len__=" << data.length() << '\n' << std::flush;
    kvTree = nested->data_nested_root();
    kv = kvTree->items()->Get(0);
    key = str(kv->key());
    auto string = kv->value_as_String();
    std::cout
      << "nested kvTree ptr=" << ZuBoxPtr(kvTree).hex()
      << " key=" << key
      << " value_type=" << EnumNameValue(kv->value_type())
      << " value=" << str(string) << '\n' << std::flush;
    std::cout << ZfbField::Load<zfbtest::Test>{test} << '\n' << std::flush;
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
