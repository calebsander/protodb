"use strict";
// Type declarations for messages common to database files and the TCP protocol
Object.defineProperty(exports, "__esModule", { value: true });
exports.CollectionType = void 0;
var CollectionType;
(function (CollectionType) {
    CollectionType[CollectionType["ITEM"] = 0] = "ITEM";
    CollectionType[CollectionType["HASH"] = 1] = "HASH";
    CollectionType[CollectionType["LIST"] = 2] = "LIST";
    CollectionType[CollectionType["SORTED"] = 3] = "SORTED";
})(CollectionType = exports.CollectionType || (exports.CollectionType = {}));
