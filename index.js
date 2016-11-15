'use strict';

const TYPES = ['insert', 'update', 'delete', 'change'];
const CHANNEL_SEPARATOR = '__';
var debug = require('debug')('nuodata-pubsub-api');

var fromChannel = function(channel) {
  var channel = channel.split(CHANNEL_SEPARATOR);
  var target = channel[1] + '.' + channel[2];
  var type = channel[0];
  var event = target + ' ' + type;

  return {
    target: target,
    type: type,
    event: event,
    room: event
  };
};

module.exports = function(logger, server, cn) {
  var io = require('socket.io')(server);

  // instantiate database connection, listen to notification on NEW connection
  const pgp = require('pg-promise')({
    connect: function (client, dc, isFresh) {
      client.on('notification', function (data) {
        var info = fromChannel(data.channel);
        // broadcast to the room
        io.sockets.in(info.room).emit(info.event, data.payload);
      });
    }
  });

  const pg = pgp(cn);

  io.on('connection', function (socket) {
    socket.on('disconnecting', function () {
      debug('Disconnecting from socket, trying to remove from channels');
      for (var room in socket.rooms) {

      }
    });

    // handle subscribe events
    socket.on('subscribe', function (data) {
      debug('Subscribing on channel');

      data = JSON.parse(data);

      if (!TYPES.includes(data.type)) {
        socket.emit('error', 'Type can only one of ' + TYPES.join(', '));
      }

      if (data.target === undefined) {
        return socket.emit('error', 'Target cannot be null');
      }


      pg.connect()
        .then(function (db) {
          debug('Subscribing on database channel');

          db.one('SELECT nuodata_pubsub.subscribe($1, $2);', [data.target, data.type])
            .then(function (data) {
              var info = fromChannel(data.subscribe);

              socket.join(info.room);

              // return the channel name
              debug('Subscribed to database channel');
              socket.emit('subscribed', info);
            })
            .catch(function (error) {
              socket.emit('error', 'Database error, could not subscribe to channel');
            });
        });
    });

    socket.on('unsubscribe', function (data) {
    });
  });
};