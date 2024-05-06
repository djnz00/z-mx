#include <zlib/ZtHexDump.hh>

#include <zlib/ZtlsRandom.hh>

#include <zlib/ZvUserDB.hh>

using namespace Zfb;
using namespace ZvUserDB;
namespace fbs = ZvUserDB::fbs;

int main()
{
  Ztls::Random rng;

  rng.init();

  ZmRef<ZiIOBuf<>> iobuf;

  {
    Mgr mgr(&rng, 12, 6, 30, 1<<20 /* 1Mb */ );

    ZtString passwd, secret;

    mgr.bootstrap("admin", "admin", passwd, secret);

    std::cout << "passwd: " << passwd << "\nsecret: " << secret << '\n';

    IOBuilder b;

    b.Finish(mgr.save_(b));

    uint8_t *buf = b.GetBufferPointer();
    int len = b.GetSize();

    // std::cout << ZtHexDump("", buf, len);

    iobuf = b.buf();

    std::cout << ZtHexDump("\n", iobuf->data(), iobuf->length);

    if ((void *)buf != (void *)(iobuf->data()) ||
	len != (int)iobuf->length) {
      std::cerr << "FAILED - inconsistent buffers\n" << std::flush;
      return 1;
    }
  }

  {
    using namespace Load;

    auto db = fbs::GetUserDB(iobuf->data());

    auto perm = db->perms()->LookupByKey(int(fbs::ReqData::ChPass) + 1);

    if (!perm) {
      std::cerr << "READ FAILED - key lookup failed\n" << std::flush;
      return 1;
    }

    if (str(perm->name()) != "UserDB.ChPass") {
      std::cerr << "READ FAILED - wrong key\n" << std::flush;
      return 1;
    }

    Mgr mgr(&rng, 12, 6, 30, 1<<20 /* 1Mb */);

    std::cout << ZtHexDump{"\n", iobuf->data(), iobuf->length};

    if (!mgr.load_(ZuBytes{iobuf->data(), iobuf->length})) {
      std::cerr << "LOAD FAILED - failed to verify\n" << std::flush;
      return 1;
    }

    if (mgr.perm(2) != "UserDB.ChPass") {
      std::cerr << "LOAD FAILED - wrong key\n" << std::flush;
      return 1;
    }
  }

  return 0;
}
