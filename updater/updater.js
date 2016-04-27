/**
 * A script to create vector tiles from generated GeoJSON, using a sat-api
 * database as the source.
 */
'use strict';

import elasticsearch from 'elasticsearch';
import iron_worker from 'iron_worker'; // eslint-disable-line camelcase
import { crossTest, warpArray } from './warp.js';
import { series } from 'async';
import { writeFileSync } from 'fs';
import upload from 'mapbox-upload';
import moment from 'moment';
import { join } from 'path';
import { ReadableSearch } from 'elasticsearch-streams';

let Winston = require('winston');
let winston = new (Winston.Logger)({
  transports: [
    new (Winston.transports.Console)({'timestamp': true, colorize: true})
  ]
});

const params = iron_worker.params() || {};
const groupings = params.groupings || [];
const mapboxToken = params.mapboxToken;
const esURL = params.esURL || '';
const queryLimit = params.queryLimit || 1000;

// Set logging level
winston.level = params.logLevel || 'info';

// Create ES client
const es = new elasticsearch.Client({
  host: esURL
});

/**
* Zero padding function
*
* @param {string} n The string to be padded
* @param {number} c The length of final string
* @return {string} A zero-padded string
*/
export function zp (n, c) {
  var s = String(n);
  if (s.length < c) {
    return zp(`0${n}`, c);
  } else {
    return s;
  }
}

/**
 * The main run loop for the script, this will start up Mongo, grab the data,
 * build the GeoJSON and upload to Mabox for rendering as a vector tile.
 */
export function doTheThing () {
  // Grab all daytime items given our regex pattern and return the geojson
  let buildGeoJSON = function (pattern, skipToday = false, cb) {
    const searchExec = function searchExec (from, callback) {
      es.search({
        index: 'sat-api',
        type: 'sentinel2',
        q: `date:[${pattern}-01-01 TO ${pattern}-12-31]`,
        from: from,
        size: queryLimit
      }, callback);
    };

    // Create ES results stream
    const rs = new ReadableSearch(searchExec);
    let count = 0;
    let geojson = {
      type: 'FeatureCollection',
      features: []
    };

    rs.on('data', (d) => {
      d = d._source;
      // Make sure we have all needed fields
      if (d.cloud_coverage === undefined || !d.date || !d.tile_geometry ||
          d.utm_zone === undefined || !d.latitude_band || !d.grid_square || !d.path) {
        return;
      }

      const feature = {
        type: 'Feature',
        geometry: d.tile_geometry,
        properties: {
          c: d.cloud_coverage,
          d: Number(moment(d.date, 'YYYY-MM-DD').format('DDD')),
          s: `${zp(d.utm_zone, 2)}${d.latitude_band}${d.grid_square}${d.path.slice(-1)}`
        }
      };

      geojson.features.push(feature);

      count++;
      if (count % 1000 === 0) {
        winston.info(`Processed ${count} records.`);
        // cb(geojson);
      }
    });
    rs.on('error', (e) => {
      winston.error(e);
    });
    rs.on('end', () => {
      winston.verbose('ES stream ended');
      winston.verbose('World wrapping started');
      for (const f of geojson.features) {
        const coordArray = f.geometry.coordinates[0];
        const crosses = crossTest(coordArray);
        if (crosses) {
          f.geometry.coordinates[0] = warpArray(coordArray);
        }
      }
      winston.verbose('World wrapping complete');
      cb(geojson);
    });
  };

  // Build up task groups
  let groups = groupings.map((g) => {
    return function (done) {
      winston.info(`Running for grouping ${g.pattern} and uploading to ${g.mapboxID}`);
      buildGeoJSON(g.pattern, g.skipToday, (geojson) => {
        // Save it to disk so we can upload to Mapbox, we're already blocked
        // here so just do it sync
        let filename = `${g.mapboxID}.geojson`;
        winston.info(`Saving geojson to disk at ${filename}`);
        writeFileSync(filename, JSON.stringify(geojson));

        // We have the geojson, upload to Mapbox
        winston.info(`Started uploading to ${g.mapboxAccount}.${g.mapboxID}`);
        let progress = upload({
          file: join(__dirname, `/${filename}`),
          account: g.mapboxAccount,
          accesstoken: mapboxToken,
          mapid: `${g.mapboxAccount}.${g.mapboxID}`
        });

        progress.once('error', (err) => {
          done(err);
        });

        progress.on('progress', (p) => {
          winston.verbose(`Upload progress for ${g.mapboxID}: ${p.percentage}`);
        });

        progress.once('finished', () => {
          winston.info(`Finished uploading to ${g.mapboxID}`);
          done(null);
        });
      });
    };
  });

  // Run in a series
  series(groups, (err, results) => {
    if (err) {
      winston.error('Exiting with an error');
      winston.error(err);
      process.exit(1);
    }

    winston.info('All vectorization processes have finished.');
    process.exit(0);
  });
}
