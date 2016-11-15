var debug = require('debug')('nuodata-pubsub:test')
describe('Socket test', function() {
  var socket;

  before(function(done) {
    _test.db.boot().then(function() {
      _test.server.listen(_test.socket.port, function () {
        debug('Starting server');
        done();
      });
    });
  });

  beforeEach(function() {
    debug('Connect to socket %s', _test.socket.url);
    socket = _test.socket.client.connect(_test.socket.url, _test.socket.options);
  });

  it('Should subscribe to uuid_data change channel', function (done) {
    socket.on('subscribed', function (info) {
      socket.on(info.event, function (data) {
        data = JSON.parse(data);
        debug('%j', data)

        data.should.have.keys(['uuid']);
        done();
      });

      _test.pg
        .any('SELECT nuodata_pubsub.watch($1);', ['uuid_data'])
        .then(function () {
          return _test.pg.any('INSERT INTO uuid_data VALUES (gen_random_uuid());');
        });
    });

    socket.emit('subscribe', JSON.stringify({
      type: 'change',
      target: 'public.uuid_data'
    }));
  });

  afterEach(function(done){
    socket.on('disconnect', function() {
      done();
    });
    socket.disconnect(true);
  });

  after(function (done) {
    _test.db.clear().then(function() {
      _test.server.close();
      done();
    });
  });
});
