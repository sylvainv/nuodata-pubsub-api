.PHONY: test unit integration

MOCHA=node_modules/.bin/mocha
REPORTER = spec

test:
	NODE_ENV=testing DATABASE_URL=postgres://travis_ci_user@127.0.0.1:5432/travis_ci_db \
	$(MOCHA) --require test/integration/bootstrap.js --reporter $(REPORTER) test/integration/index.js -t 10000