# QL Client (Node)
Simple streaming client for Trino built to handle large result sets with a minimal memory footprint.
This package leverages [`stream-json`](https://github.com/uhop/stream-json) for blazing fast JSON parsing.


## Installation
1) Add package to your project
```
npm install @eqworks/trino-client-node
```
or
```
yarn add @eqworks/trino-client-node
```
2) Import package into your project
```js
const { Client, PartStreamer } = require('@eqworks/trino-client-node')
```


## Examples
```js
const { pipeline } = require('stream')

const { S3 } = require('aws-sdk')

const { Client, PartStreamer } = require('@eqworks/trino-client-node')


const s3Client = new S3()

// instantiate client
const client = new Client({ host: 'trino.locus.place', username: '***', password: '***' })

// run query
const myQuery = new Promise((resolve, reject) => {
  const queryStream = client.query('SELECT * FROM some_table')

  // split results into parts of ~20MB compressed and stream to S3
  const parts = []
  const partHandler = (partIndex, partStream) => {
    parts.push(
      s3Client.upload({
        Bucket: 'test-kevin2',
        Key: `ql-trino-test/my_test/${partIndex + 1}`,
        Body: partStream,
        ContentEncoding: 'gzip',
        ContentType: 'application/json'
      }).promise()
    )
  }

  pipeline(
    [
      queryStream,
      new PartStreamer({ partSizeMB: 20, partHandler }),
    ],
    (err) => {
      if (err) {
        return reject(err)
      }
      // resolve myQuery promise when all parts have been written to S3
      Promise.all(parts).then(resolve).catch(reject)
    }
  )
}
```
