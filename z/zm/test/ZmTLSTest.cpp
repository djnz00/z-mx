#include <zlib/ZuBox.hpp>

#include <zlib/ZmPlatform.hpp>

Zm::ThreadID getTID() {
  return Zm::getTID();
}

int main()
{
  std::cout << ZuBoxed(getTID()) << '\n';
  return 0;
}
