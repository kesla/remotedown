var test       = require('tape')
  , testCommon = require('abstract-leveldown/testCommon')
  , remoteDOWN    = require('./common').factory
  , testBuffer = require('memdown/testdata_b64')

/*** compatibility with basic LevelDOWN API ***/

// meh require('abstract-leveldown/abstract/leveldown-test').args(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/open-test').args(remoteDOWN, test, testCommon)
// require('abstract-leveldown/abstract/open-test').open(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/del-test').all(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/get-test').all(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/put-test').all(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/put-get-del-test').all(remoteDOWN, test, testCommon, testBuffer)

require('abstract-leveldown/abstract/batch-test').all(remoteDOWN, test, testCommon)
// require('abstract-leveldown/abstract/chained-batch-test').all(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/close-test').close(remoteDOWN, test, testCommon)

require('abstract-leveldown/abstract/iterator-test').all(remoteDOWN, test, testCommon)

// require('abstract-leveldown/abstract/ranges-test').all(remoteDOWN, test, testCommon)
