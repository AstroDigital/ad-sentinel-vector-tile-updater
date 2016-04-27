'use strict';

let test = require('tape');
import { zp } from '../updater';

test('Test zero padding', (t) => {
  t.equal(zp('a', 3), '00a');
  t.equal(zp('aa', 3), '0aa');
  t.equal(zp('aaa', 3), 'aaa');
  t.equal(zp('a', 1), 'a');
  t.end();
});
