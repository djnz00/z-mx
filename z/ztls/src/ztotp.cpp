#include <iostream>

#include <zlib/ZuBox.hpp>

#include <zlib/ZtArray.hpp>

#include <zlib/ZtlsBase32.hpp>
#include <zlib/ZtlsTOTP.hpp>

static void usage() {
  std::cout
    << "usage: ztotp BASE32\n\n"
    << "app URI is otpauth://totp/ID@DOMAIN?secret=BASE32&issuer=ISSUER\n"
    << std::flush;
  ::exit(1);
}

int main(int argc, char **argv)
{
  ZtArray<uint8_t> secret;
  if (argc != 2) usage();
  int n = strlen(argv[1]);
  secret.length(Ztls::Base32::declen(n));
  secret.length(
      Ztls::Base32::decode(secret.data(), secret.length(), argv[1], n));
  auto code = Ztls::TOTP::calc(secret.data(), secret.length());
  std::cout << ZuBoxed(code).fmt<ZuFmt::Right<6>>() << '\n';
}
