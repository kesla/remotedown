# remotedown[![build status](https://secure.travis-ci.org/kesla/remotedown.svg)](http://travis-ci.org/kesla/remotedown)

A leveldown-compatible library to connect to a remote leveldown

[![NPM](https://nodei.co/npm/remotedown.png?downloads&stars)](https://nodei.co/npm/remotedown/)

[![NPM](https://nodei.co/npm-dl/remotedown.png)](https://nodei.co/npm/remotedown/)

## Stability

Unstable: Expect patches and features, possible api changes.

## Installation

```
npm install remotedown
```

## Example

### Input

```javascript
var remoteDOWN = require('./remotedown')
  , serverDb = require('memdown')('/does/not/matter')

  , server = remoteDOWN.server(serverDb)
  , client = remoteDOWN.client()

server.pipe(client.createRpcStream()).pipe(server)

client.batch(
    [{ key: new Buffer('beep'), value: new Buffer('boop'), type: 'put' }]
  , function () {
      serverDb.get(new Buffer('beep'), function (err, value) {
        console.log('value saved in server:', value.toString())
      })
    }
)
```

### Output

```
value saved in server: boop
```
