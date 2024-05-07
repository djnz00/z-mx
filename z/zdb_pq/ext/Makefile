# https://www.postgresql.org/docs/current/extend-pgxs.html

PG_CONFIG = pg_config

extension_version = 0

EXTENSION = zpq
MODULE_big = zpq
OBJS = decimal.o
SHLIB_LINK = -L../../zu/src -lZu
DATA_built = zpq--$(extension_version).sql

REGRESS = init hash operators misc drop
REGRESS_OPTS = --inputdir=test

#EXTRA_CLEAN += 

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

zpq--$(extension_version).sql: zpq.sql hash.sql operators.sql
	cat $^ >$@
