#include <iostream>
#include <sstream>

#include <zlib/ZuBitStream.hh>
#include <zlib/ZuArrayN.hh>
#include <zlib/ZuStringN.hh>
#include <zlib/ZuHex.hh>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

void print(uint64_t i)
{
  std::cout << ZuBoxed(i).hex() << '\n';
}

int main()
{
  ZuArrayN<uint8_t, 100> buf;
  ZuOBitStream o{buf.data(), buf.data() + buf.size()};

  o.out<3>(0x5);
  o.out<2>(0x1);
  o.out<3>(0x5);
  o.out(0x55, 8);
  o.out(0x555, 11);
  o.out(0x155, 9);
  o.out(0x555, 12);
  o.out(0x5555, 16);
  o.out(0x15555, 19);
  o.out(0x5555, 17);
  o.out(0x55555, 20);
  o.out<3>(0x5);
  o.out(0x1555555, 26);
  o.out(1, 1);

  o.finish();
  buf.length(o.pos() - buf.data());

  ZuStringN<200> hex;
  hex.length(ZuHex::encode({hex.data(), hex.size()}, buf));
  std::cout << hex << '\n';

  ZuIBitStream i{buf.data(), buf.data() + buf.length()};

  CHECK(i.in<3>() == 0x5);
  CHECK(i.in<2>() == 0x1);
  CHECK(i.in<3>() == 0x5);
  CHECK(i.in(8) == 0x55);
  CHECK(i.in(11) == 0x555);
  CHECK(i.in(9) == 0x155);
  CHECK(i.in(12) == 0x555);
  CHECK(i.in(16) == 0x5555);
  CHECK(i.in(19) == 0x15555);
  CHECK(i.in(17) == 0x5555);
  CHECK(i.in(20) == 0x55555);
  CHECK(i.in<3>() == 0x5);
  CHECK(i.in(26) == 0x1555555);
  CHECK(i.in(1) == 1);
  CHECK(!i.avail<8>());

  ZuOBitStream o2{i, buf.data() + buf.size()};
  
  o2.out<1>(1);

  o2.finish();
  buf.length(o2.pos() - buf.data());

  hex.length(ZuHex::encode({hex.data(), hex.size()}, buf));
  std::cout << hex << '\n';

  ZuIBitStream i2{buf.data(), buf.data() + buf.length()};

  CHECK(i2.in<3>() == 0x5);
  CHECK(i2.in<2>() == 0x1);
  CHECK(i2.in<3>() == 0x5);
  CHECK(i2.in(8) == 0x55);
  CHECK(i2.in(11) == 0x555);
  CHECK(i2.in(9) == 0x155);
  CHECK(i2.in(12) == 0x555);
  CHECK(i2.in(16) == 0x5555);
  CHECK(i2.in(19) == 0x15555);
  CHECK(i2.in(17) == 0x5555);
  CHECK(i2.in(20) == 0x55555);
  CHECK(i2.in<3>() == 0x5);
  CHECK(i2.in(26) == 0x1555555);
  CHECK(i2.in(1) == 1);
  CHECK(i2.in<1>() == 1);
  CHECK(!i2.avail<8>());
}
