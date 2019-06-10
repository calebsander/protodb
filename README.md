# protoDB

[![Build Status](https://travis-ci.org/calebsander/protodb.svg?branch=master)](https://travis-ci.org/calebsander/protodb)
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
```ts
import protobufjs from 'protobufjs'
import {ProtoDBClient} from 'protodb'

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
  - `listDelete` (O(log n)) - Remove the value at a given index in the list, or the last value in the list if no index is provided.
  - `listGet` (O(log n)) - Get the value at a given index in the list.
  - `listInsert` (O(log n)) - Insert a value at a given index in the list, or after the last value in the list if no index is provided.
  - `listSet` (O(log n)) - Set a value at a given index in the list.
  - `listSize` (O(1)) - Get the number of values in the list.
  - `listIter` - Create an iterator over the values in the list or a given range of indices in the list.
  - `listIterBreak` - Finish the given iteration.
  - `listIterNext` - Get the next value in a given iteration, if one exists.
- **Sorted** (coming soon): a collection of values sorted by numeric or string keys

All data structures have corresponding `create` and `drop` commands, e.g. `itemCreate` and `itemDrop`.

## Running the database

Once version `1.0.0` is released, you will be able to install this package from npm: `npm install protodb`.
The package includes both the database implementation and a client interface for it.
To run the database, simply execute `protodb` from an npm script, or run `node_modules/.bin/protodb` in a terminal.
By default, the database will store its files in a `data` folder in the current directory and listen on port 9000.
You can configure these from the command line (run `protodb --help` for details).

## API

Commands are issued by sending protocol buffers to the TCP server hosted by `protoDB`.
Responses are also sent as protocol buffers across the TCP socket.
The protocol buffer types defining requests and responses are in [`pb/request.ts`](pb/request.ts).
The npm package exports a client interface that wraps the TCP protocol.
See [the example](#example) above which uses this interface.
The interface allows you to connect to a `protoDB` database on any host (default `localhost`) and port (default 9000), which are passed to the `ProtoDBClient` constructor.
The `ProtoDBClient` object has a method corresponding to each `protoDB` command:
```ts
class ProtoDBClient {
  constructor(port?: number, host?: string)

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
}
```
If the server reports an error when executing a command, it will cause the `Promise` to reject with a `ProtoDBError`.
The `ProtoDBError` constructor is also exported from the npm package.

The client is written in TypeScript and exports typings so it can be used from a TypeScript project.
It can also be used in Node.js without TypeScript:
```js
const {ProtoDBClient, ProtoDBError} = require('protodb')

const client = new ProtoDBClient
// ...
```