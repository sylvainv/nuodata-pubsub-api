#!/bin/sh

psql -c 'create user travis_ci_user login superuser' -U $PGUSER
psql -c 'create database travis_ci_db;' -U $PGUSER
psql -U travis_ci_user -d travis_ci_db -f test/integration/clear.sql
