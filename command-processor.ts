import {inspect} from 'util'
import {Writer} from 'protobufjs'
import {getCollections} from './collections'
import * as hash from './collections/hash'
import * as item from './collections/item'
import * as list from './collections/list'
import * as sorted from './collections/sorted'
import {Collections} from './pb/interface'
import {
	IterParams,
	NameParams,
	NameIndexParams,
	NameIndexValueParams,
	NameKeyParams,
	NameKeyValueParams,
	NameOptionalIndexParams,
	NameOptionalIndexValueParams,
	NameRangeParams,
	NameSortedKeyParams,
	NameSortedKeyValueParams,
	NameValueParams,
	OptionalIndex,
	BytesResponse,
	ItemsListResponse,
	ErrorResponse,
	IterResponse,
	ListResponse,
	OptionalBytesResponse,
	OptionalPairResponse,
	SizeResponse,
	VoidResponse,
	commandType,
	bytesResponseType,
	itemsListResponseType,
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

const getIndex = (index: OptionalIndex) =>
	'none' in index ? undefined : index.value

async function runList(): Promise<ListResponse> {
	let collections: Collections
	try {
		collections = await getCollections as Collections
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
		item.set(name, value)
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
		hash.remove(name, key)
			.then(_ => ({}))
			.catch(errorToString)
const runHashGet =
	({name, key}: NameKeyParams): Promise<OptionalBytesResponse> =>
		hash.get(name, key)
			.then<OptionalBytesResponse>(data => data ? {data} : {none: {}})
			.catch(errorToString)
const runHashSet =
	({name, key, value}: NameKeyValueParams): Promise<VoidResponse> =>
		hash.set(name, key, value)
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
const runListDelete =
	({name, index}: NameOptionalIndexParams): Promise<VoidResponse> =>
		list.remove(name, getIndex(index))
			.then(_ => ({}))
			.catch(errorToString)
const runListGet =
	({name, index}: NameIndexParams): Promise<BytesResponse> =>
		list.get(name, index)
			.then(data => ({data}))
			.catch(errorToString)
const runListInsert =
	({name, index, value}: NameOptionalIndexValueParams): Promise<VoidResponse> =>
		list.insert(name, getIndex(index), value)
			.then(_ => ({}))
			.catch(errorToString)
const runListSet =
	({name, index, value}: NameIndexValueParams): Promise<VoidResponse> =>
		list.set(name, index, value)
			.then(_ => ({}))
			.catch(errorToString)
const runListSize =
	({name}: NameParams): Promise<SizeResponse> =>
		list.size(name)
			.then(size => ({size}))
			.catch(errorToString)
const runListIter =
	({name, start, end}: NameRangeParams): Promise<IterResponse> =>
		list.iter(name, getIndex(start), getIndex(end))
			.then(iter => ({iter}))
			.catch(errorToString)
function runListIterBreak({iter}: IterParams): VoidResponse {
	try {
		list.iterBreak(iter)
	}
	catch (e) {
		return errorToString(e)
	}
	return {}
}
const runListIterNext =
	({iter}: IterParams): Promise<OptionalBytesResponse> =>
		list.iterNext(iter)
			.then(data => data ? {data} : {none: {}})
			.catch(errorToString)
const runSortedCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		sorted.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runSortedDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		sorted.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
const runSortedGet =
	({name, key}: NameSortedKeyParams): Promise<ItemsListResponse> =>
		sorted.get(name, {elements: key})
			.then(items => ({items: {items}}))
			.catch(errorToString)
const runSortedInsert =
	({name, key, value}: NameSortedKeyValueParams): Promise<VoidResponse> =>
		sorted.insert(name, {elements: key}, value)
			.then(_ => ({}))
			.catch(errorToString)

async function runCommand(data: Uint8Array): Promise<Uint8Array> {
	const command = commandType.toObject(
		commandType.decode(data),
		{longs: Number, defaults: true}
	)
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
	else if ('listDelete' in command) {
		writer = voidResponseType.encode(await runListDelete(command.listDelete))
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
	else if ('listIter' in command) {
		writer = iterResponseType.encode(await runListIter(command.listIter))
	}
	else if ('listIterBreak' in command) {
		writer = voidResponseType.encode(runListIterBreak(command.listIterBreak))
	}
	else if ('listIterNext' in command) {
		writer = optionalBytesResponseType.encode(await runListIterNext(command.listIterNext))
	}
	else if ('sortedCreate' in command) {
		writer = voidResponseType.encode(await runSortedCreate(command.sortedCreate))
	}
	else if ('sortedDrop' in command) {
		writer = voidResponseType.encode(await runSortedDrop(command.sortedDrop))
	}
	else if ('sortedGet' in command) {
		writer = itemsListResponseType.encode(await runSortedGet(command.sortedGet))
	}
	else if ('sortedInsert' in command) {
		writer = voidResponseType.encode(await runSortedInsert(command.sortedInsert))
	}
	else {
		const unreachable: never = command
		unreachable
		throw new Error(`Unexpected command: ${inspect(command)}`)
	}
	return writer.finish()
}

let runningCommand = Promise.resolve()
export function executeCommand(data: Uint8Array): Promise<Uint8Array> {
	const result = runningCommand.then(_ => runCommand(data))
	runningCommand = result.then(_ => {})
	return result
}