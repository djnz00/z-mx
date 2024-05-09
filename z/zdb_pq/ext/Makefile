# https://www.postgresql.org/docs/current/extend-pgxs.html

PG_CONFIG = pg_config

extension_version = 0

EXTENSION = zmx
MODULE_big = zmx
OBJS = zdecimal.o magic.o
#PG_CPPFLAGS =
SHLIB_LINK = -lZu
DATA_built = zmx--$(extension_version).sql

#REGRESS = init zdecimal drop
#REGRESS_OPTS = --inputdir=test

#EXTRA_CLEAN += 

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

zmx--$(extension_version).sql: zdecimal.sql
	cat $^ >$@
