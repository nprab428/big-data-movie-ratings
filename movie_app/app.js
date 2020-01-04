'use strict';
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const hbase = require('hbase-rpc-client');
const port = 3181;
const BigIntBuffer = require('bigint-buffer');

// Serve static files
app.use(express.static('public'));

// Connect to hbase
var client = hbase({
  zookeeperHosts: [
    'mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:2181'
  ],
  zookeeperRoot: '/hbase-unsecure'
});

client.on('error', function(err) {
  console.log(err);
});


// Series of helper functions
function toImdbUrl(idStr) {
  const numZeros = 7 - idStr.length
  return `http://www.imdb.com/title/tt${'0'.repeat(numZeros)}${idStr}`
}

function colToString(row, col) {
  return row.cols[col].value.toString()
}

function colToBigInt(row, col) {
  return Number(BigIntBuffer.toBigIntBE(row.cols[col].value))
}


// Get all ratings for a movie
app.get('/get_movie_ratings.html', function(req, res) {
  const title = req.query['movie'].toLowerCase();
  // use scan on table to prefix match on title
  const scan = client.getScanner('nprabhu_movie_titles_to_ids');
  // reverse the scan so most recent listing of movie is sorted first
  // in case of duplicate titles
  scan.setFilter({prefixFilter: {prefix: `${title}:::`}});
  scan.setReversed();

  scan.toArray(function(err, rows) {
    assert.ok(!err, `get returned an error: ${err}`);
    if (rows.length == 0) {
      res.send('<html><body>No such movie in database</body></html>');
      return;
    }
    // show ratings only for most recent movie listing
    const imdbId = colToString(rows[0], 'titlemap:imdbid');
    const get = new hbase.Get(imdbId);

    // query movie ratings table to get movie ratings for selectred imdb id
    client.get('nprabhu_movie_ratings_count_hbase', get, function(err, row) {
      assert.ok(!err, `get returned an error: ${err}`);
      const total = colToBigInt(row, 'ratings:total');
      const official_title = colToString(row, 'ratings:title');

      function rating_percentage(rating) {
        const ratingCount = colToBigInt(row, `ratings:${rating}`);
        if (ratingCount == 0) {
          return '-'
        }
        return (100 * ratingCount / total).toFixed(1);
      }

      function rating_average() {
        return ((0.5 * colToBigInt(row, 'ratings:half') +
                 1 * colToBigInt(row, 'ratings:one') +
                 1.5 * colToBigInt(row, 'ratings:one_and_a_half') +
                 2 * colToBigInt(row, 'ratings:two') +
                 2.5 * colToBigInt(row, 'ratings:two_and_a_half') +
                 3 * colToBigInt(row, 'ratings:three') +
                 3.5 * colToBigInt(row, 'ratings:three_and_a_half') +
                 4 * colToBigInt(row, 'ratings:four') +
                 4.5 * colToBigInt(row, 'ratings:four_and_a_half') +
                 5 * colToBigInt(row, 'ratings:five')) /
                total)
            .toFixed(3)
      }

      // display rating percentages in table
      var template = filesystem.readFileSync('ratings.mustache').toString();
      var html = mustache.render(template, {
        title: official_title,
        url: toImdbUrl(imdbId),
        year: colToString(row, 'ratings:year'),
        half: rating_percentage('half'),
        one: rating_percentage('one'),
        one_and_a_half: rating_percentage('one_and_a_half'),
        two: rating_percentage('two'),
        two_and_a_half: rating_percentage('two_and_a_half'),
        three: rating_percentage('three'),
        three_and_a_half: rating_percentage('three_and_a_half'),
        four: rating_percentage('four'),
        four_and_a_half: rating_percentage('four_and_a_half'),
        five: rating_percentage('five'),
        total: total,
        average: rating_average()
      });
      res.send(html);
    });
  });
});

// Get top/bottom n movies by genre
app.get('/get_movie_rankings.html', function(req, res) {
  const count = req.query['count'];
  const rankType = req.query['rank-type'];
  const genre = req.query['genre'];
  const scan =
    client.getScanner('nprabhu_movie_ratings_average_by_genre_hbase');
  // reverse scan list ratings from highest to lowest i.e. best to worst
  if (rankType == 'best') {
    scan.setReversed();
  }
  // prefix match in table by the genre type
  scan.setFilter({prefixFilter: {prefix: genre.toUpperCase()}});
  scan.toArray(function(err, rows) {
    if (!rows) {
      res.send('<html><body>No movies in rankings table for selected genre</body></html>');
      return;
    }
    // for each movie display the following info
    const renderedRows = rows.slice(0, count).map((row, ix) => {
      return {
        index: (ix + 1).toString(),
        title: colToString(row, 'ratings:title'),
        url: toImdbUrl(colToString(row, 'ratings:imdbid')),
        year: colToString(row, 'ratings:year'),
        rating: parseFloat(colToString(row, 'ratings:avgRating')).toFixed(3)
      };
    })
    var template = filesystem.readFileSync('rankings.mustache').toString();
    var html = mustache.render(template, {row: renderedRows});
    res.send(html);
  });
});


app.listen(port);
