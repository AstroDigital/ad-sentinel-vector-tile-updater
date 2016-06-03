/**
 * A script to create vector tiles from generated GeoJSON, using a sat-api
 * database as the source.
 */
'use strict';

import elasticsearch from 'elasticsearch';
import iron_worker from 'iron_worker'; // eslint-disable-line camelcase
import { crossTest, warpArray } from './warp.js';
import { parallelLimit } from 'async';
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
const types = params.types || [];
const mapboxToken = params.mapboxToken;
const mapboxAccount = params.mapboxAccount;
const taskLimit = params.taskLimit || 1;
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
  let buildGeoJSON = function (pattern, type, cb) {
    // Build query based on type
    let query = `date:${pattern}`;
    if (type === 'landsat8') {
      // Add a filter for day/night
      query = `${query} AND dayOrNight:'DAY'`;
    } else if (type === 'sentinel2') {
      // Nothing extra needed
    } else {
      winston.error('Unknown data search type');
      process.exit(1);
    }

    const searchExec = function searchExec (from, callback) {
      es.search({
        index: 'sat-api',
        type: type,
        q: query,
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
      // Make sure we have all needed fields, dependent on type
      if (type === 'landsat8') {
        if (d.cloud_coverage === undefined || !d.date || !d.data_geometry ||
            d.path === undefined || d.row === undefined) {
          return;
        }
      } else if (type === 'sentinel2') {
        if (d.cloud_coverage === undefined || !d.date || !d.tile_geometry ||
            d.utm_zone === undefined || !d.latitude_band || !d.grid_square || !d.path) {
          return;
        }
      }

      // Get properties dependent on type
      let geometry;
      let scene;
      let date;
      let year;
      if (type === 'landsat8') {
        geometry = d.data_geometry;
        scene = `${d.sceneID.substring(1, 2)}${d.sceneID.substring(18, 19)}${d.sceneID.substring(20, 21)}${zp(d.path, 3)}${zp(d.row, 3)}`;
        date = Number(d.sceneID.substring(13, 16));
        year = d.sceneID.substring(11, 13);
      } else if (type === 'sentinel2') {
        geometry = d.tile_geometry;
        scene = `${zp(d.utm_zone, 2)}${d.latitude_band}${d.grid_square}${d.path.slice(-1)}`;
        date = Number(moment(d.date, 'YYYY-MM-DD').format('DDD'));
        year = d.date.substring(2, 4);
      }

      const feature = {
        type: 'Feature',
        geometry: geometry,
        properties: {
          c: d.cloud_coverage,
          d: date,
          s: scene,
          y: year
        }
      };

      geojson.features.push(feature);

      count++;
      if (count % 1000 === 0) {
        winston.info(`Processed ${count} records.`);
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
        // TODO: remove after testing
        f.geometry.coordinates[0].forEach((c) => {
          if (c[0] >= 180 || c[0] <= -180) {
            console.log('Out of bounds!');
            console.log(JSON.stringify(f));
          }
        });
      }
      winston.verbose('World wrapping complete');
      cb(geojson);
    });
  };

  // Build up task groups
  let groupings = [];
  types.forEach((t) => {
    // Starting from Jan 1 of the start year and go to end of month of current
    // month.
    let date = moment([t.startYear, 0, 1]);
    groupings.push({
      pattern: `[${date.startOf('month').format('YYYY-MM-DD')} TO ${moment().endOf('month').format('YYYY-MM-DD')}]`,
      mapboxID: t.baseName,
      type: t.type
    });
  });
  let groups = groupings.map((g) => {
    return function (done) {
      winston.info(`Running for grouping ${g.type} matching ${g.pattern} and uploading to ${g.mapboxID}`);
      buildGeoJSON(g.pattern, g.type, (geojson) => {
        // If geojson is empty, we can exit now
        if (geojson.features.length === 0) {
          winston.info(`No features found for ${g.type} matching ${g.pattern}`);
          return done(null);
        }

        // Save it to disk so we can upload to Mapbox, we're already blocked
        // here so just do it sync
        let filename = `${g.mapboxID}.geojson`;
        winston.info(`Saving geojson to disk at ${filename}`);
        writeFileSync(filename, JSON.stringify(geojson));

        // We have the geojson, upload to Mapbox
        winston.info(`Started uploading to ${mapboxAccount}.${g.mapboxID}`);
        let progress = upload({
          file: join(__dirname, `/${filename}`),
          account: mapboxAccount,
          accesstoken: mapboxToken,
          mapid: `${mapboxAccount}.${g.mapboxID}`
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
  parallelLimit(groups, taskLimit, (err, results) => {
    if (err) {
      winston.error('Exiting with an error');
      winston.error(err);
      process.exit(1);
    }

    winston.info('All vectorization processes have finished.');
    process.exit(0);
  });
}
