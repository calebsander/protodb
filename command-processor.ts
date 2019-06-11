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
	NameKeyRangeParams,
	NameKeyValueParams,
	NameOptionalIndexValueParams,
	NameRangeParams,
	NameSortedKeyParams,
	NameSortedKeyValueParams,
	NameValueParams,
	OptionalIndex,
	OptionalKey,
	BytesResponse,
	ErrorResponse,
	IterResponse,
	ListResponse,
	OptionalBytesResponse,
	OptionalPairResponse,
	OptionalSortedPairResponse,
	SizeResponse,
	SortedPairListResponse,
	VoidResponse,
	commandType,
	bytesResponseType,
	iterResponseType,
	listResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	optionalSortedPairResponse,
	sizeResponseType,
	sortedPairListResponseType,
	voidResponseType
} from './pb/request'

function makeErrorResponse(err: Error): ErrorResponse {
	console.error(err)
	const {name, message} = err
	return {error: `${name}: ${message}`}
}

const getIndex = (index: OptionalIndex) =>
	'none' in index ? undefined : index.value
const getKey = (key: OptionalKey) =>
	'none' in key ? undefined : key.value.elements

async function runList(): Promise<ListResponse> {
	let collections: Collections
	try {
		collections = await getCollections as Collections
	}
	catch (e) {
		// istanbul ignore next
		return makeErrorResponse(e)
	}
	return {db: {collections}}
}
const runHashCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		hash.create(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runHashDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		hash.drop(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runHashDelete =
	({name, key}: NameKeyParams): Promise<VoidResponse> =>
		hash.remove(name, key)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runHashGet =
	({name, key}: NameKeyParams): Promise<OptionalBytesResponse> =>
		hash.get(name, key)
			.then<OptionalBytesResponse>(data => data ? {data} : {none: {}})
			.catch(makeErrorResponse)
const runHashSet =
	({name, key, value}: NameKeyValueParams): Promise<VoidResponse> =>
		hash.set(name, key, value)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runHashSize =
	({name}: NameParams): Promise<SizeResponse> =>
		hash.size(name)
			.then(size => ({size}))
			.catch(makeErrorResponse)
const runHashIter =
	({name}: NameParams): Promise<IterResponse> =>
		hash.iter(name)
			.then(iter => ({iter}))
			.catch(makeErrorResponse)
function runHashIterBreak({iter}: IterParams): VoidResponse {
	try {
		hash.iterBreak(iter)
	}
	catch (e) {
		return makeErrorResponse(e)
	}
	return {}
}
const runHashIterNext =
	({iter}: IterParams): Promise<OptionalPairResponse> =>
		hash.iterNext(iter)
			.then(pair => pair ? {pair} : {})
			.catch(makeErrorResponse)
const runItemCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		item.create(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runItemDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		item.drop(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runItemGet =
	({name}: NameParams): Promise<BytesResponse> =>
		item.get(name)
			.then(data => ({data}))
			.catch(makeErrorResponse)
const runItemSet =
	({name, value}: NameValueParams): Promise<VoidResponse> =>
		item.set(name, value)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		list.create(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		list.drop(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListDelete =
	({name, index}: NameIndexParams): Promise<VoidResponse> =>
		list.remove(name, index)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListGet =
	({name, index}: NameIndexParams): Promise<BytesResponse> =>
		list.get(name, index)
			.then(data => ({data}))
			.catch(makeErrorResponse)
const runListInsert =
	({name, index, value}: NameOptionalIndexValueParams): Promise<VoidResponse> =>
		list.insert(name, getIndex(index), value)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListSet =
	({name, index, value}: NameIndexValueParams): Promise<VoidResponse> =>
		list.set(name, index, value)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runListSize =
	({name}: NameParams): Promise<SizeResponse> =>
		list.size(name)
			.then(size => ({size}))
			.catch(makeErrorResponse)
const runListIter =
	({name, start, end}: NameRangeParams): Promise<IterResponse> =>
		list.iter(name, getIndex(start), getIndex(end))
			.then(iter => ({iter}))
			.catch(makeErrorResponse)
function runListIterBreak({iter}: IterParams): VoidResponse {
	try {
		list.iterBreak(iter)
	}
	catch (e) {
		return makeErrorResponse(e)
	}
	return {}
}
const runListIterNext =
	({iter}: IterParams): Promise<OptionalBytesResponse> =>
		list.iterNext(iter)
			.then(data => data ? {data} : {none: {}})
			.catch(makeErrorResponse)
const runSortedCreate =
	({name}: NameParams): Promise<VoidResponse> =>
		sorted.create(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runSortedDrop =
	({name}: NameParams): Promise<VoidResponse> =>
		sorted.drop(name)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runSortedDelete =
	({name, key}: NameSortedKeyParams): Promise<VoidResponse> =>
		sorted.remove(name, key)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runSortedGet =
	({name, key}: NameSortedKeyParams): Promise<SortedPairListResponse> =>
		sorted.get(name, key)
			.then(pairs => ({pairs: {pairs}}))
			.catch(makeErrorResponse)
const runSortedInsert =
	({name, key, value}: NameSortedKeyValueParams): Promise<VoidResponse> =>
		sorted.insert(name, key, value)
			.then(_ => ({}))
			.catch(makeErrorResponse)
const runSortedSize =
	({name}: NameParams): Promise<SizeResponse> =>
		sorted.size(name)
			.then(size => ({size}))
			.catch(makeErrorResponse)
const runSortedIter =
	({name, start, end, inclusive}: NameKeyRangeParams): Promise<IterResponse> =>
		sorted.iter(name, inclusive, getKey(start), getKey(end))
			.then(iter => ({iter}))
			.catch(makeErrorResponse)
function runSortedIterBreak({iter}: IterParams): VoidResponse {
	try {
		sorted.iterBreak(iter)
	}
	catch (e) {
		return makeErrorResponse(e)
	}
	return {}
}
const runSortedIterNext =
	({iter}: IterParams): Promise<OptionalSortedPairResponse> =>
		sorted.iterNext(iter)
			.then(pair => pair ? {pair} : {})
			.catch(makeErrorResponse)

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
		writer = optionalPairResponseType.encode(
			await runHashIterNext(command.hashIterNext)
		)
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
		writer = optionalBytesResponseType.encode(
			await runListIterNext(command.listIterNext)
		)
	}
	else if ('sortedCreate' in command) {
		writer = voidResponseType.encode(await runSortedCreate(command.sortedCreate))
	}
	else if ('sortedDrop' in command) {
		writer = voidResponseType.encode(await runSortedDrop(command.sortedDrop))
	}
	else if ('sortedDelete' in command) {
		writer = voidResponseType.encode(await runSortedDelete(command.sortedDelete))
	}
	else if ('sortedGet' in command) {
		writer = sortedPairListResponseType.encode(
			await runSortedGet(command.sortedGet)
		)
	}
	else if ('sortedInsert' in command) {
		writer = voidResponseType.encode(await runSortedInsert(command.sortedInsert))
	}
	else if ('sortedSize' in command) {
		writer = sizeResponseType.encode(await runSortedSize(command.sortedSize))
	}
	else if ('sortedIter' in command) {
		writer = iterResponseType.encode(await runSortedIter(command.sortedIter))
	}
	else if ('sortedIterBreak' in command) {
		writer = voidResponseType.encode(
			await runSortedIterBreak(command.sortedIterBreak)
		)
	}
	else if ('sortedIterNext' in command) {
		writer = optionalSortedPairResponse.encode(
			await runSortedIterNext(command.sortedIterNext)
		)
	}
	// istanbul ignore next
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