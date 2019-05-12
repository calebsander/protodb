import {inspect} from 'util'
import {Writer} from 'protobufjs'
import {getCollections} from './collections'
import * as item from './collections/item'
import * as hash from './collections/hash'
import * as list from './collections/list'
import {Collections} from './pb/db'
import {
	IterParams,
	NameParams,
	NameIndexParams,
	NameIndexValueParams,
	NameKeyParams,
	NameKeyValueParams,
	NameValueParams,
	BytesResponse,
	ErrorResponse,
	IterResponse,
	ListResponse,
	OptionalBytesResponse,
	OptionalPairResponse,
	SizeResponse,
	VoidResponse,
	commandType,
	bytesResponseType,
	iterResponseType,
	listResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	sizeResponseType,
	voidResponseType
} from './pb/request'

function errorToString(err: Error): ErrorResponse {
	console.error(err)
	const {name, message} = err
	return {error: `${name}: ${message}`}
}

async function runList(): Promise<ListResponse> {
	let collections: Collections
	try {
		collections = await getCollections
	}
	catch (e) {
		return errorToString(e)
	}
	return {db: {collections}}
}
const runItemCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		item.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runItemDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		item.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
const runItemGet =
	({name}: NameParams): Promise<BytesResponse> =>
		item.get(name)
			.then(data => ({data}))
			.catch(errorToString)
const runItemSet =
	({name, value}: NameValueParams): Promise<VoidResponse> =>
		item.set(name, value || new Uint8Array)
			.then(_ => ({}))
			.catch(errorToString)
const runHashCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		hash.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runHashDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		hash.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
const runHashDelete =
	({name, key}: NameKeyParams): Promise<VoidResponse> =>
		hash.remove(name, key || new Uint8Array)
			.then(_ => ({}))
			.catch(errorToString)
const runHashGet =
	({name, key}: NameKeyParams): Promise<OptionalBytesResponse> =>
		hash.get(name, key || new Uint8Array)
			.then<OptionalBytesResponse>(data => data ? {data} : {none: {}})
			.catch(errorToString)
const runHashSet =
	({name, key, value}: NameKeyValueParams): Promise<VoidResponse> =>
		hash.set(name, key || new Uint8Array, value || new Uint8Array)
			.then(_ => ({}))
			.catch(errorToString)
const runHashSize =
	({name}: NameParams): Promise<SizeResponse> =>
		hash.size(name)
			.then(size => ({size}))
			.catch(errorToString)
const runHashIter =
	({name}: NameParams): Promise<IterResponse> =>
		hash.iter(name)
			.then(iter => ({iter}))
			.catch(errorToString)
function runHashIterBreak({iter}: IterParams): VoidResponse {
	try {
		hash.iterBreak(iter)
	}
	catch (e) {
		return errorToString(e)
	}
	return {}
}
const runHashIterNext =
	({iter}: IterParams): Promise<OptionalPairResponse> =>
		hash.iterNext(iter)
			.then(item => item ? {item} : {})
			.catch(errorToString)
const runListCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		list.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runListDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		list.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
async function runListGet(
	{name, index}: NameIndexParams
): Promise<BytesResponse> {
	if (index === undefined) throw new Error('Missing index')

	let data: Uint8Array
	try {
		data = await list.get(name, index)
	}
	catch (e) {
		return errorToString(e)
	}
	return {data}
}
const runListInsert =
	({name, index, value}: NameIndexValueParams): Promise<VoidResponse> =>
		list.insert(name, index, value || new Uint8Array)
			.then(_ => ({}))
			.catch(errorToString)
async function runListSet(
	{name, index, value}: NameIndexValueParams
): Promise<VoidResponse> {
	if (index === undefined) throw new Error('Missing index')

	try {
		await list.set(name, index, value || new Uint8Array)
	}
	catch (e) {
		return errorToString(e)
	}
	return {}
}
const runListSize =
	({name}: NameParams): Promise<SizeResponse> =>
		list.size(name)
			.then(size => ({size}))
			.catch(errorToString)

export async function runCommand(data: Uint8Array): Promise<Uint8Array> {
	const command = commandType.toObject(commandType.decode(data), {longs: Number})
	let writer: Writer
	if ('list' in command) {
		writer = listResponseType.encode(await runList())
	}
	else if ('itemCreate' in command) {
		writer = voidResponseType.encode(await runItemCreate(command.itemCreate))
	}
	else if ('itemDrop' in command) {
		writer = voidResponseType.encode(await runItemDrop(command.itemDrop))
	}
	else if ('itemGet' in command) {
		writer = bytesResponseType.encode(await runItemGet(command.itemGet))
	}
	else if ('itemSet' in command) {
		writer = voidResponseType.encode(await runItemSet(command.itemSet))
	}
	else if ('hashCreate' in command) {
		writer = voidResponseType.encode(await runHashCreate(command.hashCreate))
	}
	else if ('hashDrop' in command) {
		writer = voidResponseType.encode(await runHashDrop(command.hashDrop))
	}
	else if ('hashDelete' in command) {
		writer = voidResponseType.encode(await runHashDelete(command.hashDelete))
	}
	else if ('hashGet' in command) {
		writer = optionalBytesResponseType.encode(await runHashGet(command.hashGet))
	}
	else if ('hashSet' in command) {
		writer = voidResponseType.encode(await runHashSet(command.hashSet))
	}
	else if ('hashSize' in command) {
		writer = sizeResponseType.encode(await runHashSize(command.hashSize))
	}
	else if ('hashIter' in command) {
		writer = iterResponseType.encode(await runHashIter(command.hashIter))
	}
	else if ('hashIterBreak' in command) {
		writer = voidResponseType.encode(runHashIterBreak(command.hashIterBreak))
	}
	else if ('hashIterNext' in command) {
		writer = optionalPairResponseType.encode(await runHashIterNext(command.hashIterNext))
	}
	else if ('listCreate' in command) {
		writer = voidResponseType.encode(await runListCreate(command.listCreate))
	}
	else if ('listDrop' in command) {
		writer = voidResponseType.encode(await runListDrop(command.listDrop))
	}
	else if ('listGet' in command) {
		writer = bytesResponseType.encode(await runListGet(command.listGet))
	}
	else if ('listInsert' in command) {
		writer = voidResponseType.encode(await runListInsert(command.listInsert))
	}
	else if ('listSet' in command) {
		writer = voidResponseType.encode(await runListSet(command.listSet))
	}
	else if ('listSize' in command) {
		writer = sizeResponseType.encode(await runListSize(command.listSize))
	}
	else {
		const unreachable: never = command
		unreachable
		throw new Error('Unexpected command: ' + inspect(command))
	}
	return writer.finish()
}