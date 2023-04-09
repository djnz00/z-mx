#!/bin/sh
libtool exec valgrind --vgdb=yes --vgdb-error=0 ./CmdTest cmdtest.crt cmdtest.key user.db 127.0.0.1 9000
