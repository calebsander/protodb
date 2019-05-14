# protoDB

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
```
hashCreate users
hashSet users users.proto {"userId":1234} {"firstName":"Qwerty","lastName":"Uiop","age":30}
hashGet users users.proto {"userId":1234}
```
And it will retrieve the corresponding value:
```
{ firstName: 'Qwerty', lastName: 'Uiop', age: 30 }
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

## API

Commands are issued by sending protocol buffers to the TCP server hosted by `protoDB`.
Responses are also sent as protocol buffers across the TCP socket.
The protocol buffer types defining requests and responses are in [`pb/request.ts`](pb/request.ts).
[`test/client.ts`](test/client.ts) implements a basic command prompt that wraps this binary API.
See [the example](#example) above which uses this text interface.
An exported interface allowing other libraries to issue commands to `protoDB` is coming soon.