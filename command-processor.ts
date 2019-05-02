import {getCollections} from './collections'
import * as item from './collections/item'
import * as hash from './collections/hash'
import * as types from './sb-types/request'

const errorToString = ({name, message}: Error) =>
	({error: `${name}: ${message}`})

async function runList(): Promise<types.ListResponse> {
	try {
		const dbCollections = await getCollections()
		const collections: types.Collection[] = []
		for (const [name, {type}] of dbCollections) collections.push({name, type})
		return {collections}
	}
	catch (e) {
		return errorToString(e)
	}
}
const runItemCreate =
	({name}: types.ItemCreateCommand): Promise<types.VoidResponse> =>
		item.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runItemDrop =
	({name}: types.ItemDropCommand): Promise<types.VoidResponse> =>
		item.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
const runItemGet =
	({name}: types.ItemGetCommand): Promise<types.BytesResponse> =>
		item.get(name)
			.then(data => ({data}))
			.catch(errorToString)
const runItemSet =
	({name, value}: types.ItemSetCommand): Promise<types.VoidResponse> =>
		item.set(name, value)
			.then(_ => ({}))
			.catch(errorToString)
const runHashCreate =
	({name}: types.HashCreateCommand): Promise<types.VoidResponse> =>
		hash.create(name)
			.then(_ => ({}))
			.catch(errorToString)
const runHashDrop =
	({name}: types.HashDropCommand): Promise<types.VoidResponse> =>
		hash.drop(name)
			.then(_ => ({}))
			.catch(errorToString)
const runHashGet =
	({name, key}: types.HashGetCommand): Promise<types.OptionalBytesResponse> =>
		hash.get(name, key)
			.then(data => ({data}))
			.catch(errorToString)
const runHashSet =
	({name, key, value}: types.HashSetCommand): Promise<types.VoidResponse> =>
		hash.set(name, key, value)
			.then(_ => ({}))
			.catch(errorToString)
const runHashDelete =
	({name, key}: types.HashDeleteCommand): Promise<types.VoidResponse> =>
		hash.remove(name, key)
			.then(_ => ({}))
			.catch(errorToString)
const runHashSize =
	({name}: types.HashSizeCommand): Promise<types.UnsignedResponse> =>
		hash.size(name)
			.then(value => ({value}))
			.catch(errorToString)
const runHashIter =
	({name}: types.HashIterCommand): Promise<types.IterResponse> =>
		hash.iter(name)
			.then(iter => ({iter}))
			.catch(errorToString)
const runHashIterNext =
	({iter}: types.HashIterNextCommand): Promise<types.OptionalPairResponse> =>
		hash.iterNext(iter)
			.then(item => ({item}))
			.catch(errorToString)
function runHashIterBreak({iter}: types.HashIterBreakCommand): types.VoidResponse {
	try {
		hash.iterBreak(iter)
		return {}
	}
	catch (e) {
		return errorToString(e)
	}
}

export async function runCommand(data: ArrayBuffer): Promise<ArrayBuffer> {
	const command = types.commandType.readValue(data)
	switch (command.type) {
		case 'list':
			return types.listReponseType.valueBuffer(await runList())
		case 'item_create':
			return types.voidReponseType.valueBuffer(await runItemCreate(command))
		case 'item_drop':
			return types.voidReponseType.valueBuffer(await runItemDrop(command))
		case 'item_get':
			return types.bytesResponseType.valueBuffer(await runItemGet(command))
		case 'item_set':
			return types.voidReponseType.valueBuffer(await runItemSet(command))
		case 'hash_create':
			return types.voidReponseType.valueBuffer(await runHashCreate(command))
		case 'hash_drop':
			return types.voidReponseType.valueBuffer(await runHashDrop(command))
		case 'hash_get':
			return types.optionalBytesResponseType.valueBuffer(await runHashGet(command))
		case 'hash_set':
			return types.voidReponseType.valueBuffer(await runHashSet(command))
		case 'hash_delete':
			return types.voidReponseType.valueBuffer(await runHashDelete(command))
		case 'hash_size':
			return types.unsignedResponseType.valueBuffer(await runHashSize(command))
		case 'hash_iter':
			return types.iterResponseType.valueBuffer(await runHashIter(command))
		case 'hash_iter_next':
			return types.optionalPairResponseType.valueBuffer(await runHashIterNext(command))
		case 'hash_iter_break':
			return types.voidReponseType.valueBuffer(runHashIterBreak(command))
		default:
			const unreachable: never = command
			unreachable
			console.error('Unrecognized command')
			return new ArrayBuffer(0)
	}
}