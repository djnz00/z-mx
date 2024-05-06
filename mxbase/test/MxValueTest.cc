#include <zlib/ZuStringN.hh>

#include <mxbase/MxBase.hh>

#include <iostream>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

int main()
{
  // check basic string scan
  CHECK((double)(MxValNDP{"0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"."}.fp()) == 0.0);
  CHECK((double)(MxValNDP{".0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"0."}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"0.0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-."}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-.0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-0."}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-0.0"}.fp()) == 0.0);
  CHECK((double)(MxValNDP{".", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{".0", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"0.", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"0.0", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-.", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-.0", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-0.", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"-0.0", 3}.fp()) == 0.0);
  CHECK((double)(MxValNDP{"1000.42"}.fp()) == 1000.42);
  CHECK((double)(MxValNDP{"-1000.42"}.fp()) == -1000.42);
  CHECK((double)(MxValNDP{"1000.42", 4}.fp()) == 1000.42);
  CHECK((double)(MxValNDP{"-1000.42", 4}.fp()) == -1000.42);
  // check basic value/ndp scanning
  {
    auto v = MxValNDP{"1000.42", 4};
    CHECK(v.value == 10004200);
    CHECK(v.ndp == 4);
    CHECK((double)(v.fp()) == 1000.42);
    v = MxValNDP{"-1000.42", 1};
    CHECK(v.value == -10004);
    CHECK(v.ndp == 1);
    CHECK((double)(v.fp()) == -1000.4);
  }
  // check leading/trailing zeros
  CHECK((double)(MxValNDP{"001", 10}.fp()) == 1.0);
  CHECK((double)(MxValNDP{"1.000", 10}.fp()) == 1.0);
  CHECK((double)(MxValNDP{"001.000", 10}.fp()) == 1.0);
  CHECK((double)(MxValNDP{"00.100100100", 18}.fp()) == .1001001);
  CHECK((double)(MxValNDP{"0.10010010", 18}.fp()) == .1001001);
  CHECK((double)(MxValNDP{".1001001", 18}.fp()) == .1001001);
  {
    // check basic multiply
    MxFloat v = (MxValNDP{"1000.42", 4} * MxValNDP{2.5, 2}).fp();
    CHECK((double)v == 2501.05);
    v = (MxValNDP{"-1000.42", 3} * MxValNDP{2.5, 3}).fp();
    CHECK((double)v == -2501.05);
    v = (MxValNDP{"1000.42", 2} * MxValNDP{-2.5, 4}).fp();
    CHECK((double)v == -2501.05);
    v = (MxValNDP{"-1000.42", 1} * MxValNDP{-2.5, 5}).fp();
    CHECK((double)v == 2501.0);
  }
  {
    // check overflow multiply
    auto v = (MxValNDP{"10", 16} * MxValNDP{"10", 16}).value;
    CHECK(!*v);
    v = (MxValNDP{"10", 15} * MxValNDP{"10", 15}).value;
    CHECK((double)(MxValNDP{v, 15}.fp()) == 100.0);
  }
  {
    // check underflow multiply
    auto v = (MxValNDP{".1", 1} * MxValNDP{".1", 1}).value;
    CHECK(!v);
    v = (MxValNDP{".1", 2} * MxValNDP{".1", 2}).value;
    CHECK((double)(MxValNDP{v, 2}.fp()) == .01);
  }
  // check overflow strings
  CHECK((!*MxValNDP{"", 4}));
  CHECK((!*MxValNDP{"1", 19}));
  CHECK((!*MxValNDP{"100", 17}));
  CHECK((!*MxValNDP{"1", 18}));
  // check underflow strings
  CHECK((!MxValNDP{".1", 0}));
  CHECK((!MxValNDP{"000.010", 1}));
  // check formatted printing
  {
    ZuStringN<32> s;
    s << MxValNDP{"42000.42", 2}.fmt(ZuFmt::Comma<>());
    CHECK(s == "42,000.42");
  }
}
