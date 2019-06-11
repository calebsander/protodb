"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const _1 = require(".");
const args_1 = require("../args");
const cache_1 = require("../cache");
const interface_1 = require("../pb/interface");
const item_1 = require("../pb/item");
const filename = (name) => path.join(args_1.dataDir, `${name}.item`);
async function checkIsItem(name) {
    const collections = await _1.getCollections;
    const collection = collections[name];
    if (collection !== interface_1.CollectionType.ITEM) {
        throw new Error(`Collection ${name} is not an item`);
    }
}
function create(name) {
    return _1.addCollection(name, interface_1.CollectionType.ITEM);
}
exports.create = create;
async function drop(name) {
    await checkIsItem(name);
    await Promise.all([
        _1.dropCollection(name),
        cache_1.removeFile(filename(name))
            .catch(_ => { }) // not a problem if the item was never set
    ]);
}
exports.drop = drop;
async function get(name) {
    await checkIsItem(name);
    let contents;
    try {
        contents = await cache_1.getFile(filename(name));
    }
    catch (_a) {
        throw new Error(`Item ${name} has not been set`);
    }
    const message = item_1.itemType.decodeDelimited(contents);
    return item_1.itemType.toObject(message, { defaults: true }).value;
}
exports.get = get;
async function set(name, value) {
    await checkIsItem(name);
    await cache_1.setFile(filename(name), item_1.itemType.encodeDelimited({ value }).finish());
}
exports.set = set;
