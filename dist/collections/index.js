"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const args_1 = require("../args");
const cache_1 = require("../cache");
const db_1 = require("../pb/db");
const DB_FILE = path.join(args_1.dataDir, 'db');
const loadCollections = cache_1.getFile(DB_FILE)
    .then(contents => {
    const message = db_1.dbType.decodeDelimited(contents);
    return db_1.dbType.toObject(message, { defaults: true }).collections;
})
    .catch(_ => ({}));
exports.getCollections = loadCollections;
async function saveCollections() {
    const collections = await loadCollections;
    await cache_1.setFile(DB_FILE, db_1.dbType.encodeDelimited({ collections }).finish());
}
async function addCollection(name, collection) {
    const collections = await loadCollections;
    if (name in collections) {
        throw new Error(`Collection ${name} already exists`);
    }
    collections[name] = collection;
    await saveCollections();
}
exports.addCollection = addCollection;
async function dropCollection(name) {
    const collections = await loadCollections;
    // istanbul ignore if
    if (!(name in collections)) {
        throw new Error(`Collection ${name} does not exist`);
    }
    delete collections[name];
    await saveCollections();
}
exports.dropCollection = dropCollection;
