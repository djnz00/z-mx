#include <stdlib.h>
#ifndef _WIN32
#include <alloca.h>
#endif

#include <iostream>
#include <string>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuByteSwap.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/tree_fbs.h>

using IOBuilder = Zfb::IOBuilder<>;
using IOBuf = IOBuilder::IOBuf;
std::vector<ZmRef<IOBuf>> bufs;

namespace ZfbTree {
using namespace Zfb;

namespace Save {
  using namespace Zfb::Save;

  template <typename B, typename ...Args>
  inline auto tree(B &b, Args &&... args) {
    return fbs::CreateTree(b, ZuFwd<Args>(args)...);
  }
  // Example:
  // itemInt32Vec(fbb, "integers", 2, [](unsigned i) { return i; });
#define ZfbTree_Primitive(ctype, fbstype) \
  template <typename B> \
  inline auto item##fbstype(B &b, ZuString key, ctype value) { \
    return fbs::CreateItem(b, str(b, key), fbs::Value_##fbstype, \
	b.CreateStruct(fbs::fbstype{value}).Union()); \
  } \
  template <typename B, typename L> \
  inline auto item##fbstype##Vec(B &b, ZuString key, unsigned n, L l) { \
    return fbs::CreateItem(b, str(b, key), fbs::Value_##fbstype##Vec, \
	fbs::Create##fbstype##Vec(b, pvectorIter(b, n, l)).Union()); \
  }
  ZfbTree_Primitive(int, Int32)
  ZfbTree_Primitive(unsigned, UInt32)
  ZfbTree_Primitive(int64_t, Int64)
  ZfbTree_Primitive(uint64_t, UInt64)
  ZfbTree_Primitive(double, Double)
  template <typename B>
  inline auto itemString(B &b, ZuString key, ZuString value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_String,
	str(b, value).Union());
  }
  // Example:
  // itemStringVec(b, "strings", 2,
  //   [](unsigned i) { return ZtString{} << "value" << i; });
  template <typename B, typename L>
  inline auto itemStringVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_StringVec,
	fbs::CreateStringVec(b, strVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemBitmap(B &b, ZuString key, const ZmBitmap &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_Bitmap,
	bitmap(b, value).Union());
  }
  // Example:
  // itemBitmapVec(b, "bitmaps", 2, [] <template B> (B &b, unsigned i) {
  //   return bitmap(b, ZmBitmap{static_cast<uint64_t>(i)});
  // }
  template <typename B, typename L>
  inline auto itemBitmapVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_BitmapVec,
	fbs::CreateBitmapVec(b, vectorIter<Bitmap>(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemDecimal(B &b, ZuString key, const ZuDecimal &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_Decimal,
	b.CreateStruct(decimal(value)).Union());
  }
  // Example:
  // itemDecimalVec(b, "decimals", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) Decimal{decimal(ZuDecimal{i})};
  // }
  template <typename B, typename L>
  inline auto itemDecimalVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DecimalVec,
	fbs::CreateDecimalVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemDateTime(B &b, ZuString key, const ZtDate &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DateTime,
	b.CreateStruct(dateTime(value)).Union());
  }
  // Example:
  // itemDateTimeVec(b, "dateTimes", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) DateTime{dateTime(ZtDate{2023, 2, i})};
  // }
  template <typename B, typename L>
  inline auto itemDateTimeVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DateTimeVec,
	fbs::CreateDateTimeVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemIP(B &b, ZuString key, const ZiIP &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IP,
	b.CreateStruct(ip(value)).Union());
  }
  // Example:
  // itemIPVec(b, "ips", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) IP{ip(ZiIP{ZtString{} << "192.168.0." << i})};
  // }
  template <typename B, typename L>
  inline auto itemIPVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IPVec,
	fbs::CreateIPVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemID(B &b, ZuString key, const ZuID &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_ID,
	b.CreateStruct(id(value)).Union());
  }
  // Example:
  // itemIDVec(b, "ids", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) fbs::ID{id(ZuID{ZtString{} << i})};
  // }
  template <typename B, typename L>
  inline auto itemIDVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IDVec,
	fbs::CreateIDVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B, typename ...Args>
  inline auto nested(B &b, Args &&... args) {
    return fbs::CreateNested(b,
      nest(b, [...args = ZuFwd<Args>(args)] <typename B_> (B_ &b) mutable {
	return fbs::CreateTree(b, args...);
      }));
  }
  template <typename B, typename ...Args>
  inline auto itemNested(B &b, ZuString key, Args &&... args) {
    using namespace Save;
    return fbs::CreateItem(b, str(b, key), fbs::Value_Nested,
	nested(b, ZuFwd<Args>(args)...).Union());
  }
  template <typename B, typename ...Args>
  inline auto itemNested_(B &b, ZuString key, Args &&... args) {
    using namespace Save;
    return fbs::CreateItem(b, str(b, key), fbs::Value_Nested,
	fbs::CreateNested(b, ZuFwd<Args>(args)...).Union());
  }
  // Example:
  // itemNestedVec(b, "nesteds", 2, [] <template B> (B &b, unsigned i) {
  //   return nested(b, vector<fbs::Item>(b, itemString(b, "key", "value")));
  // }
  template <typename B, typename L>
  inline auto itemNestedVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_NestedVec,
	fbs::CreateBitmapVec(b, vectorIter<fbs::Nested>(b, n, l)).Union());
  }
} // Save

} // ZfbTree

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
