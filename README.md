# protoDB

[![Build Status](https://github.com/calebsander/protodb/workflows/Node.js%20CI/badge.svg)](https://github.com/calebsander/protodb/actions)
[![Coverage Status](https://coveralls.io/repos/github/calebsander/protodb/badge.svg?branch=master)](https://coveralls.io/github/calebsander/protodb)

A structured byte-buffer database.
Created as a CS 123 project at Caltech in 2019.

![icon](icon.svg)

## Example

`protoDB` can store any type of data that is serialized into byte-buffers.
A library like [Protocol Buffers](https://developers.google.com/protocol-buffers/) makes it easy to define serialization formats for any particular type of value.
Consider the following `users.proto` file that defines key and values types for a hash map:
```protobuf
syntax = "proto3";

message KeyType {
  uint32 userId = 1;
}

message ValueType {
  string firstName = 1;
  string lastName = 2;
  uint32 age = 3;
}
```
Using `protoDB`, we can create a hash map with these keys and values:
```js
const protobufjs = require('protobufjs')
const {ProtoDBClient} = require('proto-database')

async function main() {
  const types = await protobufjs.load('users.proto')
  const KeyType = types.lookupType('KeyType'),
        ValueType = types.lookupType('ValueType')

  const client = new ProtoDBClient
  await client.hashCreate('users')

  const key = KeyType.encode({userId: 1234}).finish()
  const value = ValueType.encode({
    firstName: 'Qwerty',
    lastName: 'Uiop',
    age: 30
  }).finish()
  await client.hashSet('users', key, value)

  const retrieved = await client.hashGet('users', key)
  console.log(ValueType.decode(retrieved))

  await client.close()
}

main()
```
And it will retrieve the corresponding value:
```
ValueType { firstName: 'Qwerty', lastName: 'Uiop', age: 30 }
```

## Motivation

`protoDB` is designed to solve several shortcomings of SQL databases:
- Whereas SQL databases store all data in unordered multisets and require index structures to access them efficiently, data structures are first-class objects in `protoDB`.
The data structures in `protoDB` are modeled on the [Redis's core collection types](https://redis.io/topics/data-types).
- `protoDB` is data-agnostic: it supports storage and retrieval of byte-buffers, leaving the serialization format up to the client.
This means that `protoDB` supports any type of data serialized by any client, whereas SQL databases are designed around a fixed set of primitive types.
- Commands are sent to `protoDB` as protocol buffers rather than strings.
This allows commands to serialized more compactly than in SQL and to be parsed more quickly by the server.

## Supported data structures

- **Item**: a single value
  - `itemGet` - Get the item's current value. Throws an error if the item is unset.
  - `itemSet` - Set the item's value.
- **Hash**: an extendible hash table mapping keys to values.
If the values are empty buffers, it can be used as a hash set.
  - `hashDelete` (O(1)) - Delete the key-value pair corresponding to a given key, if one exists.
  - `hashGet` (O(1)) - Get the value corresponding to a given key, if one exists.
  - `hashSet` (amortized O(1)) - Set the value that a given key maps to.
  - `hashSize` (O(1)) - Get the number of key-value pairs in the map.
  - `hashIter` - Create an iterator over the key-value pairs in the map.
  - `hashIterBreak` - Finish the given iteration.
  - `hashIterNext` - Get the next key-value pair in a given iteration, if one exists.
- **List**: a sequence of values with log-time insertion, deletion, and retrieval at any index.
Implemented like a B+ tree to achieve the best of contiguous and linked list representations.
Can be used as a queue, stack, or deque.
  - `listDelete` (O(log n)) - Remove the value at a given index in the list.
  - `listGet` (O(log n)) - Get the value at a given index in the list.
  - `listInsert` (O(log n)) - Insert a value at a given index in the list, or after the last value in the list if no index is provided.
  - `listSet` (O(log n)) - Set a value at a given index in the list.
  - `listSize` (O(1)) - Get the number of values in the list.
  - `listIter` - Create an iterator over a range of indices in the list.
  - `listIterBreak` - Finish the given iteration.
  - `listIterNext` - Get the next value in a given iteration, if one exists.
- **Sorted**: a collection of values sorted by keys, which are represented as tuples of numbers and strings.
Any prefix of the keys may be used as a query key; for example, if the keys are `[int, string]`, valid query keys are `[]`, `[int]`, and `[int, string]`.
Implemented as a B+ tree.
  - `sortedDelete` (O(log n)) - Remove the value with the lowest key matching the query key.
  - `sortedGet` (O(log n + m), where m is the number of pairs matched) - Get all key-value pairs whose keys match the query key, ordered by key.
  - `sortedInsert` (O(log n)) - Add a key-value pair to the collection. If the key is already present, a "uniquifier" is added to it.
  - `sortedSize` (O(1)) - Get the number of values in the collection.
  - `sortedIter` - Create an iterator over a range of keys in the collection.
  - `sortedIterBreak` - Finish the given iteration.
  - `sortedIterNext` - Get the next key-value pair in a given iteration, if one exists.

All data structures have corresponding `create` and `drop` commands, e.g. `itemCreate` and `itemDrop`.

Detailed documentation for these commands can be found in the [client interface JSDoc comments](client/index.ts).

## Running the database

Both the database server and the client interface are included in the [npm package](https://www.npmjs.com/package/proto-database).
You can install it with `npm install proto-database`.
To run the database, simply execute `protodb` from an npm script, or run `$(npm bin)/protodb` in a terminal.
To gracefully shut down the database, send it a `SIGINT`, e.g. press CTRL+c in the terminal.
By default, the database will store its files in a `data` folder in the current directory and listen on port 9000.
You can configure these from the command line (run `protodb --help` for details).

## API

Commands are sent as protocol buffers to the TCP server hosted by `protoDB`.
Responses are also serialized as protocol buffers.
The protocol buffer types defining requests and responses are in [`pb/request.proto`](pb/request.proto).
The npm package exports a `Promise`-based client interface that wraps this TCP protocol.
See [the example](#example) above which uses this interface.
The interface allows you to connect to a `protoDB` database on any host (default `localhost`) and port (default 9000), which are passed to the `ProtoDBClient` constructor.
The `ProtoDBClient` object has a method corresponding to each `protoDB` command:
```ts
class ProtoDBClient {
  constructor(port?: number, host?: string)

  close(): Promise<void>

  list(): Promise<DB>

  itemCreate(name: string): Promise<void>
  itemDrop(name: string): Promise<void>
  itemGet(name: string): Promise<Uint8Array>
  itemSet(name: string, value: ArrayBuffer | Uint8Array): Promise<void>

  hashCreate(name: string): Promise<void>
  hashDrop(name: string): Promise<void>
  hashDelete(name: string, key: ArrayBuffer | Uint8Array): Promise<void>
  hashGet(name: string, key: ArrayBuffer | Uint8Array): Promise<Uint8Array | null>
  hashSet(name: string, key: ArrayBuffer | Uint8Array, value: ArrayBuffer | Uint8Array): Promise<void>
  hashSize(name: string): Promise<number>
  hashIter(name: string): Promise<Uint8Array>
  hashIterBreak(iter: Uint8Array): Promise<void>
  hashIterNext(iter: Uint8Array): Promise<KeyValuePair | null>

  listCreate(name: string): Promise<void>
  listDrop(name: string): Promise<void>
  listDelete(name: string, index?: number): Promise<void>
  listGet(name: string, index: number): Promise<Uint8Array>
  listInsert(name: string, value: ArrayBuffer | Uint8Array, index?: number): Promise<void>
  listSet(name: string, index: number, value: ArrayBuffer | Uint8Array): Promise<void>
  listSize(name: string): Promise<number>
  listIter(name: string, start?: number, end?: number): Promise<Uint8Array>
  listIterBreak(iter: Uint8Array): Promise<void>
  listIterNext(iter: Uint8Array): Promise<Uint8Array | null>

  sortedCreate(name: string): Promise<void>
  sortedDrop(name: string): Promise<void>
  sortedDelete(name: string, key: KeyElement[]): Promise<void>
  sortedGet(name: string, key: KeyElement[]): Promise<SortedKeyValuePair[]>
  sortedInsert(name: string, key: KeyElement[], value: ArrayBuffer | Uint8Array): Promise<void>
  sortedSize(name: string): Promise<number>
  sortedIter(name: string, start?: KeyElement[], end?: KeyElement[], inclusive = false): Promise<Uint8Array>
  sortedIterBreak(iter: Uint8Array): Promise<void>
  sortedIterNext(iter: Uint8Array): Promise<SortedKeyValuePair | null>
}
```
If the server reports an error when executing a command, it will cause the `Promise` to reject with a `ProtoDBError`.
The `ProtoDBError` constructor is also exported from the npm package.

The client is written in TypeScript and exports typings so it can be used from a TypeScript project.
You can use the following import statement in TypeScript:
```ts
import {ProtoDBClient, ProtoDBError, CollectionType} from 'proto-database'

const client = new ProtoDBClient
// ...
```

`protoDB` doesn't support concurrent access.
Commands are queued in the order they are received and then processed in sequence.

## Data structures

### Hash

Hashes use [extendible hash table](https://en.wikipedia.org/wiki/Extendible_hashing)s to map keys to values.
This minimizes the work needed to grow the hash table: when a bucket overflows, it can usually be split without modifying the rest of the hash table; if the bucket already represents a single hash value, the directory must be duplicated, but no other buckets need to be modified.
There is a `*.hash.directory` file that stores the header and the hash directory, which maps hash values to bucket indices, and a `*.hash.buckets` file with a page for each bucket.

### List

The list data structure is a hybrid between a [rope](https://en.wikipedia.org/wiki/Rope_(data_structure)) and a [B-tree](https://en.wikipedia.org/wiki/B-tree).
Each leaf node is just an array of values.
Each inner node stores an array of sublists, specified by their root pages and lengths.
This allows for efficient navigation to a particular index because large portions of the list can be skipped.
For example, if the root has children of length 4,000, 5,000, and 3,000, an access of the 10,000th element becomes an access of the 1,000th element of the third child.
Insertion and deletion at any index in the list are also fast, since only the affected leaf and its ancestors' lengths need to be updated.

A `*.list` file has a header on its first page and a leaf or inner page on each subsequent page.
Adjacent nodes are coalesced into a single node when possible, so the file also maintains a linked list of free pages to use when a new node is needed.

### Sorted

Sorted maps are stored in standard [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)s.
The leaves are strung into a singly-linked list to make iteration faster.
A `*.sorted` file has a header on its first page and a leaf or inner page on each subsequent page.
Adjacent nodes are coalesced into a single node when possible, so the file also maintains a linked list of free pages to use when a new node is needed.