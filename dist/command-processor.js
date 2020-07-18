"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.processCommand = void 0;
const util_1 = require("util");
const collections_1 = require("./collections");
const hash = __importStar(require("./collections/hash"));
const item = __importStar(require("./collections/item"));
const list = __importStar(require("./collections/list"));
const sorted = __importStar(require("./collections/sorted"));
const request_1 = require("./pb/request");
const queue_1 = require("./queue");
function makeErrorResponse(err) {
    console.error(err);
    const { name, message } = err;
    return { error: `${name}: ${message}` };
}
const getIndex = (index) => 'none' in index ? undefined : index.value;
const getKey = (key) => 'none' in key ? undefined : key.value.elements;
async function runList() {
    let collections;
    try {
        collections = await collections_1.getCollections;
    }
    catch (e) {
        // istanbul ignore next
        return makeErrorResponse(e);
    }
    return { db: { collections } };
}
const runHashCreate = ({ name }) => hash.create(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runHashDrop = ({ name }) => hash.drop(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runHashDelete = ({ name, key }) => hash.remove(name, key)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runHashGet = ({ name, key }) => hash.get(name, key)
    .then(data => data ? { data } : { none: {} })
    .catch(makeErrorResponse);
const runHashSet = ({ name, key, value }) => hash.set(name, key, value)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runHashSize = ({ name }) => hash.size(name)
    .then(size => ({ size }))
    .catch(makeErrorResponse);
const runHashIter = ({ name }) => hash.iter(name)
    .then(iter => ({ iter }))
    .catch(makeErrorResponse);
function runHashIterBreak({ iter }) {
    try {
        hash.iterBreak(iter);
    }
    catch (e) {
        return makeErrorResponse(e);
    }
    return {};
}
const runHashIterNext = ({ iter }) => hash.iterNext(iter)
    .then(pair => pair ? { pair } : {})
    .catch(makeErrorResponse);
const runItemCreate = ({ name }) => item.create(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runItemDrop = ({ name }) => item.drop(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runItemGet = ({ name }) => item.get(name)
    .then(data => ({ data }))
    .catch(makeErrorResponse);
const runItemSet = ({ name, value }) => item.set(name, value)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListCreate = ({ name }) => list.create(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListDrop = ({ name }) => list.drop(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListDelete = ({ name, index }) => list.remove(name, index)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListGet = ({ name, index }) => list.get(name, index)
    .then(data => ({ data }))
    .catch(makeErrorResponse);
const runListInsert = ({ name, index, value }) => list.insert(name, getIndex(index), value)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListSet = ({ name, index, value }) => list.set(name, index, value)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runListSize = ({ name }) => list.size(name)
    .then(size => ({ size }))
    .catch(makeErrorResponse);
const runListIter = ({ name, start, end }) => list.iter(name, getIndex(start), getIndex(end))
    .then(iter => ({ iter }))
    .catch(makeErrorResponse);
function runListIterBreak({ iter }) {
    try {
        list.iterBreak(iter);
    }
    catch (e) {
        return makeErrorResponse(e);
    }
    return {};
}
const runListIterNext = ({ iter }) => list.iterNext(iter)
    .then(data => data ? { data } : { none: {} })
    .catch(makeErrorResponse);
const runSortedCreate = ({ name }) => sorted.create(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runSortedDrop = ({ name }) => sorted.drop(name)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runSortedDelete = ({ name, key }) => sorted.remove(name, key)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runSortedGet = ({ name, key }) => sorted.get(name, key)
    .then(pairs => ({ pairs: { pairs } }))
    .catch(makeErrorResponse);
const runSortedInsert = ({ name, key, value }) => sorted.insert(name, key, value)
    .then(_ => ({}))
    .catch(makeErrorResponse);
const runSortedSize = ({ name }) => sorted.size(name)
    .then(size => ({ size }))
    .catch(makeErrorResponse);
const runSortedIter = ({ name, start, end, inclusive }) => sorted.iter(name, inclusive, getKey(start), getKey(end))
    .then(iter => ({ iter }))
    .catch(makeErrorResponse);
function runSortedIterBreak({ iter }) {
    try {
        sorted.iterBreak(iter);
    }
    catch (e) {
        return makeErrorResponse(e);
    }
    return {};
}
const runSortedIterNext = ({ iter }) => sorted.iterNext(iter)
    .then(pair => pair ? { pair } : {})
    .catch(makeErrorResponse);
async function runCommand(data) {
    const command = request_1.commandType.toObject(request_1.commandType.decode(data), { longs: Number, defaults: true });
    // Dispatch the command to the function that handles that type
    let writer;
    if ('list' in command) {
        writer = request_1.listResponseType.encode(await runList());
    }
    else if ('itemCreate' in command) {
        writer = request_1.voidResponseType.encode(await runItemCreate(command.itemCreate));
    }
    else if ('itemDrop' in command) {
        writer = request_1.voidResponseType.encode(await runItemDrop(command.itemDrop));
    }
    else if ('itemGet' in command) {
        writer = request_1.bytesResponseType.encode(await runItemGet(command.itemGet));
    }
    else if ('itemSet' in command) {
        writer = request_1.voidResponseType.encode(await runItemSet(command.itemSet));
    }
    else if ('hashCreate' in command) {
        writer = request_1.voidResponseType.encode(await runHashCreate(command.hashCreate));
    }
    else if ('hashDrop' in command) {
        writer = request_1.voidResponseType.encode(await runHashDrop(command.hashDrop));
    }
    else if ('hashDelete' in command) {
        writer = request_1.voidResponseType.encode(await runHashDelete(command.hashDelete));
    }
    else if ('hashGet' in command) {
        writer = request_1.optionalBytesResponseType.encode(await runHashGet(command.hashGet));
    }
    else if ('hashSet' in command) {
        writer = request_1.voidResponseType.encode(await runHashSet(command.hashSet));
    }
    else if ('hashSize' in command) {
        writer = request_1.sizeResponseType.encode(await runHashSize(command.hashSize));
    }
    else if ('hashIter' in command) {
        writer = request_1.iterResponseType.encode(await runHashIter(command.hashIter));
    }
    else if ('hashIterBreak' in command) {
        writer = request_1.voidResponseType.encode(runHashIterBreak(command.hashIterBreak));
    }
    else if ('hashIterNext' in command) {
        writer = request_1.optionalPairResponseType.encode(await runHashIterNext(command.hashIterNext));
    }
    else if ('listCreate' in command) {
        writer = request_1.voidResponseType.encode(await runListCreate(command.listCreate));
    }
    else if ('listDrop' in command) {
        writer = request_1.voidResponseType.encode(await runListDrop(command.listDrop));
    }
    else if ('listDelete' in command) {
        writer = request_1.voidResponseType.encode(await runListDelete(command.listDelete));
    }
    else if ('listGet' in command) {
        writer = request_1.bytesResponseType.encode(await runListGet(command.listGet));
    }
    else if ('listInsert' in command) {
        writer = request_1.voidResponseType.encode(await runListInsert(command.listInsert));
    }
    else if ('listSet' in command) {
        writer = request_1.voidResponseType.encode(await runListSet(command.listSet));
    }
    else if ('listSize' in command) {
        writer = request_1.sizeResponseType.encode(await runListSize(command.listSize));
    }
    else if ('listIter' in command) {
        writer = request_1.iterResponseType.encode(await runListIter(command.listIter));
    }
    else if ('listIterBreak' in command) {
        writer = request_1.voidResponseType.encode(runListIterBreak(command.listIterBreak));
    }
    else if ('listIterNext' in command) {
        writer = request_1.optionalBytesResponseType.encode(await runListIterNext(command.listIterNext));
    }
    else if ('sortedCreate' in command) {
        writer = request_1.voidResponseType.encode(await runSortedCreate(command.sortedCreate));
    }
    else if ('sortedDrop' in command) {
        writer = request_1.voidResponseType.encode(await runSortedDrop(command.sortedDrop));
    }
    else if ('sortedDelete' in command) {
        writer = request_1.voidResponseType.encode(await runSortedDelete(command.sortedDelete));
    }
    else if ('sortedGet' in command) {
        writer = request_1.sortedPairListResponseType.encode(await runSortedGet(command.sortedGet));
    }
    else if ('sortedInsert' in command) {
        writer = request_1.voidResponseType.encode(await runSortedInsert(command.sortedInsert));
    }
    else if ('sortedSize' in command) {
        writer = request_1.sizeResponseType.encode(await runSortedSize(command.sortedSize));
    }
    else if ('sortedIter' in command) {
        writer = request_1.iterResponseType.encode(await runSortedIter(command.sortedIter));
    }
    else if ('sortedIterBreak' in command) {
        writer = request_1.voidResponseType.encode(await runSortedIterBreak(command.sortedIterBreak));
    }
    else if ('sortedIterNext' in command) {
        writer = request_1.optionalSortedPairResponse.encode(await runSortedIterNext(command.sortedIterNext));
    }
    // istanbul ignore next
    else {
        const unreachable = command;
        unreachable;
        throw new Error(`Unexpected command: ${util_1.inspect(command)}`);
    }
    return writer.finish();
}
let processing = false;
const commandQueue = new queue_1.Queue();
function executeCommand({ command, callback }) {
    runCommand(command).then(response => {
        callback(response);
        let nextCommand;
        try {
            nextCommand = commandQueue.dequeue();
        }
        catch (_a) { }
        if (nextCommand)
            executeCommand(nextCommand);
        else
            processing = false;
    });
}
function processCommand(command, callback) {
    const queuedCommand = { command, callback };
    if (processing)
        commandQueue.enqueue(queuedCommand);
    else {
        processing = true;
        executeCommand(queuedCommand);
    }
}
exports.processCommand = processCommand;
