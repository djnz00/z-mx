#include <zlib/ZeLog.hh>

#include <zlib/ZvCf.hh>

int main(int argc, char **argv)
{
  if (argc < 2) {
    fputs("You must specify a input config file name.\n", stderr);
    return 1;
  }

  ZmRef<ZvCf> cf = new ZvCf();
  try {
    cf->fromFile(argv[1]);
  } catch (ZvError &e) {
    std::cerr << e << '\n';
    return 1;
  }

  std::cout << *cf;

  return 0;
}

