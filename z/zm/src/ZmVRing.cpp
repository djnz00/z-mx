    m_handle = memfd_create("ZmVRing", MFD_CLOEXEC);
    if (ftruncate(m_handle, size) < 0) { ::close(m_handle); goto error; }
    m_addr = ::mmap(
	0, size<<1, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (m_addr == MAP_FAILED || !m_addr) goto error;
    void *addr = ::mmap(
	m_addr, size,
	PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, m_handle, 0);
    if (addr != m_addr) goto error;
    addr = ::mmap(
	static_cast<uint8_t *>(m_addr) + size, size,
	PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, m_handle, 0);
    if (addr != static_cast<void *>(
	  static_cast<uint8_t *>(m_addr) + size)) goto error;

handle = 
CreateFileMapping(INVALID_HANDLE_VALUE, 0, PAGE_READWRITE, 0, size, nullptr);
    if (!handle || handle == INVALID_HANDLE_VALUE) goto error;
retry:
      m_addr = VirtualAlloc(
	  0, static_cast<DWORD>(size<<1), MEM_RESERVE, PAGE_NOACCESS);
      if (!m_addr) goto error;
      if (!VirtualFree(m_addr, 0, MEM_RELEASE)) goto error;
      void *addr = MapViewOfFileEx(
	  handle, static_cast<DWORD>(accessFlags), 0, 0,
	  static_cast<DWORD>(size), m_addr);
      if (!addr) goto retry;
      if (addr != m_addr) { UnmapViewOfFile(addr); goto retry; }
      addr = MapViewOfFileEx(
	  handle, static_cast<DWORD>(accessFlags), 0, 0,
	  static_cast<DWORD>(size),
	  static_cast<uint8_t *>(m_addr) + size);
      if (!addr) goto retry;
      if (addr != static_cast<void *>(
	    static_cast<uint8_t *>(m_addr) + size)) {
	UnmapViewOfFile(m_addr);
	UnmapViewOfFile(addr);
	goto retry;
      }

