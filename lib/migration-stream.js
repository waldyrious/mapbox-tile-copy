var stream = require('stream');
var tiletype = require('tiletype');
var mapnik = require('mapnik');

module.exports = MigrationStream;

function MigrationStream() {
  var migrationStream = new stream.Transform({ objectMode: true });
  migrationStream._transform = function(tile, enc, callback) {
    if (!tile.buffer) {
      migrationStream.push(tile);
      return callback();
    }

    var format = tiletype.type(tile.buffer);
    if (format !== 'pbf') {
      migrationStream.push(tile);
      return callback();
    }

    var vtile = new mapnik.VectorTile(tile.z, tile.x, tile.y);


    console.log('beginning');
    vtile.setData(tile.buffer, function(err) {
      console.log('middle');
      vtile.getData({compression:'gzip'},function(err, data) {
        tile.buffer = data;
        migrationStream.push(tile);
        console.log('end');
        return callback();
      });
    });
  };

  return migrationStream;
}
