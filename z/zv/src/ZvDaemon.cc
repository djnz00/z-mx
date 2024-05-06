//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// daemon-ization

#include <zlib/ZvDaemon.hh>

#include <zlib/ZeLog.hh>
#include <zlib/ZiFile.hh>

#ifndef _WIN32
#include <sys/types.h>
#include <pwd.h>
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

int ZvDaemon::init(const char *username, const char *password, int umask,
    bool daemonize, const char *pidFile)
{
#ifndef _WIN32

// set uid/gid

  if (username) {
    struct passwd *p = getpwnam(username);

    if (!p) {
      ZeLOG(Error, ZtSprintf("getpwnam(\"%s\") failed", username));
    } else {
      setregid(p->pw_gid, p->pw_gid);
      setreuid(p->pw_uid, p->pw_uid);
    }
  }

// set umask

  if (umask >= 0) ::umask(umask);

// daemon-ize

  if (daemonize) {
    close(0);

    switch (fork()) {
    case -1:
      ZeLOG(Fatal, ZtSprintf("fork() failed: %s", ZeLastError.message()));
      return Error;
      break;
    case 0:
      ZeLog::forked();
      setsid();
      break;
    default:
      _exit(0);
      break;
    }
  }

#else

  if (username || daemonize) {

    // on Windows, re-invoke the same program unless ZvDaemon is set
    bool daemon = false;

    // get path to current program
	ZtWString path((const wchar_t *)0, Zi::PathMax);
    GetModuleFileName(0, path.data(), path.size());
    path.calcLength();
    path.truncate();

    {
      wchar_t *s = _wgetenv(L"ZvDaemon");

      if (path == s) daemon = true;
    }

    if (!daemon) {
      _wputenv(ZtWString(L"ZvDaemon=") + path);

      // get command line
      ZtWString commandLine(GetCommandLine());

      STARTUPINFO si;
      memset(&si, 0, sizeof(si));
      si.cb = sizeof(si);
      if (daemonize) {
	si.dwFlags = STARTF_USESHOWWINDOW | STARTF_USESTDHANDLES;
	si.wShowWindow = SW_HIDE;
	si.hStdInput = INVALID_HANDLE_VALUE;
	si.hStdOutput = INVALID_HANDLE_VALUE;
	si.hStdError = INVALID_HANDLE_VALUE;
      }

      int flags = CREATE_UNICODE_ENVIRONMENT;
      if (daemonize) flags |= DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP;

      PROCESS_INFORMATION pi;

      if (!username) {
	// re-invoke same program
	if (!CreateProcess(path, commandLine, 0, 0, TRUE,
			   flags, 0, 0, &si, &pi)) {
	  ZeLOG(Fatal, ZtSprintf(
		"CreateProcess failed: %s", ZeLastError.message()));
	  return Error;
	}
      } else {
	// re-invoke same program as impersonated user
	HANDLE user;
	if (!LogonUser(ZtWString(username), 0, ZtWString(password),
		       LOGON32_LOGON_NETWORK, LOGON32_PROVIDER_DEFAULT,
		       &user)) {
	  ZeLOG(Fatal, ZtSprintf(
		"LogonUser failed: %s", ZeLastError.message()));
	  return Error;
	}

	HANDLE token;
	if (!DuplicateTokenEx(user,
			  TOKEN_QUERY | TOKEN_DUPLICATE | TOKEN_ASSIGN_PRIMARY,
			      0, SecurityImpersonation, TokenPrimary,
			      &token)) {
	  CloseHandle(user);
	  ZeLOG(Fatal, ZtSprintf(
		"DuplicateTokenEx failed: %s", ZeLastError.message()));
	  return Error;
	}

	CloseHandle(user);

	if (!ImpersonateLoggedOnUser(token)) {
	  CloseHandle(token);
	  ZeLOG(Fatal, ZtSprintf(
		"ImpersonateLoggedOnUser failed: %s", ZeLastError.message()));
	  return Error;
	}

	SECURITY_DESCRIPTOR sd;
	memset(&sd, 0, sizeof(sd));
	InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION);
	SetSecurityDescriptorDacl(&sd, -1, 0, 0);

	SECURITY_ATTRIBUTES sa;
	memset(&sa, 0, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.lpSecurityDescriptor = &sd;
	sa.bInheritHandle = FALSE;

	si.lpDesktop = (wchar_t *)L"Winsta0\\Default";

	if (!CreateProcessAsUser(token, path, GetCommandLine(), &sa, 0, TRUE,
				 flags, 0, 0, &si, &pi)) {
	  RevertToSelf();
	  CloseHandle(token);
	  ZeLOG(Fatal, ZtSprintf(
		"CreateProcessAsUser failed: %s", ZeLastError.message()));
	  return Error;
	}

	RevertToSelf();
	CloseHandle(token);
      }

      CloseHandle(pi.hThread);
      CloseHandle(pi.hProcess);

      ExitProcess(0);
    }
  }

#endif /* !_WIN32 */

// create / check PID file

  if (pidFile) {
    ZiFile file;
    ZeError e;
    ZtString buf;

    if (file.open(pidFile, ZiFile::Create | ZiFile::Exclusive | ZiFile::GC,
		  0644, &e) != Zi::OK) {
      if (file.open(pidFile, ZiFile::GC, 0, &e) != Zi::OK) {
	ZeLOG(Error, ZtSprintf("open(%s): %s", pidFile, e.message()));
	return Error;
      }

      buf.size(16);

      int n;

      if ((n = file.read(buf.data(), 15, &e)) < 0) {
	ZeLOG(Error, ZtSprintf("read(%s): %s", pidFile, e.message()));
	return Error;
      }

      buf.length(n);

      int pid = atoi(buf);

      if (pid > 0) {
#ifndef _WIN32
	int i = kill(pid, 0);

	if (i >= 0 || (i < 0 && errno == EPERM)) {
	  ZeLOG(Error, ZtSprintf("PID %d still running", pid));
	  return Running;
	}
#else
	HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, pid);

	if (h) {
	  CloseHandle(h);
	  ZeLOG(Error, ZtSprintf("PID %d still running", pid));
	  return Running;
	}
#endif
      }

      file.seek(0);
    }

    buf = ZuBox<int>(Zm::getPID());

    if (file.write(buf.data(), buf.length(), &e) != Zi::OK) {
      ZeLOG(Error, ZtSprintf("write(%s): %s", pidFile, e.message()));
      return Error;
    }
  }

  return OK;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
