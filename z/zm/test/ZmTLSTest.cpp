#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTime.hpp>

Zm::ThreadID getTID() {
  return Zm::getTID();
}

int main()
{
  std::cout << getTID() << '\n' << std::flush;
  while (true) Zm::sleep(1);
}
