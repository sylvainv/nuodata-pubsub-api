#!/bin/sh

psql -U travis_ci_user -d travis_ci_db -f test/integration/clear.sql
psql -c 'drop database travis_ci_db;' -U $PGUSER
psql -c 'drop user travis_ci_user; ' -U $PGUSER
