#include <zlib/ZuCArray.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxValWindow.hh>

#include <iostream>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

int main()
{
  MxValWindow vw(10, 10, 4);

  for (unsigned i = 0; i < 200; i += 2) {
    vw.add(-1000010, i);
  }
  std::cout << "mean: " << MxValNDP{vw.mean(), 4} << '\n' << std::flush;
  CHECK(vw.mean() == -500005);

  vw.add(100000, 10000000);
  std::cout << "total: " << MxValNDP{vw.total(), 4} << '\n' << std::flush;
  CHECK(vw.total() == 100000);

  for (unsigned i = 10000050; i < 10000200; i += 50) {
    vw.add(100000, i);
    std::cout << "total: " << MxValNDP{vw.total(), 4} << '\n' << std::flush;
    CHECK(vw.total() == 200000);
  }
}
