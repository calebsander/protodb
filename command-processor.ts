import {getCollections} from './collections'
import * as item from './collections/item'
import * as hash from './collections/hash'
import {
	Collection,
	commandType,
	ItemCreateCommand,
	ItemDropCommand,
	ItemGetCommand,
	ItemSetCommand,
	HashCreateCommand,
	HashDropCommand,
	HashGetCommand,
	HashSetCommand,
	BytesResponse,
	ListResponse,
	OptionalBytesResponse,
	VoidResponse,
	bytesResponseType,
	listReponseType,
	optionalBytesResponseType,
	voidReponseType
} from './sb-types/request'

const errorToString = ({name, message}: Error) =>
	({error: `${name}: ${message}`})

async function runList(): Promise<ListResponse> {
	try {
		const dbCollections = await getCollections()
		const collections: Collection[] = []
		for (const [name, {type}] of dbCollections) collections.push({name, type})
		return {collections}
	}
	catch (e) {
		return errorToString(e)
	}
}
function runItemCreate({name}: ItemCreateCommand): Promise<VoidResponse> {
	return item.create(name)
		.then(_ => ({}))
		.catch(errorToString)
}
function runItemDrop({name}: ItemDropCommand): Promise<VoidResponse> {
	return item.drop(name)
		.then(_ => ({}))
		.catch(errorToString)
}
function runItemGet({name}: ItemGetCommand): Promise<BytesResponse> {
	return item.get(name)
		.then(data => ({data}))
		.catch(errorToString)
}
function runItemSet({name, value}: ItemSetCommand): Promise<VoidResponse> {
	return item.set(name, value)
		.then(_ => ({}))
		.catch(errorToString)
}
function runHashCreate({name}: HashCreateCommand): Promise<VoidResponse> {
	return hash.create(name)
		.then(_ => ({}))
		.catch(errorToString)
}
function runHashDrop({name}: HashDropCommand): Promise<VoidResponse> {
	return hash.drop(name)
		.then(_ => ({}))
		.catch(errorToString)
}
function runHashGet({name, key}: HashGetCommand): Promise<OptionalBytesResponse> {
	return hash.get(name, key)
		.then(data => ({data}))
		.catch(errorToString)
}
function runHashSet({name, key, value}: HashSetCommand): Promise<VoidResponse> {
	return hash.set(name, key, value)
		.then(_ => ({}))
		.catch(errorToString)
}

export async function runCommand(data: ArrayBuffer): Promise<ArrayBuffer> {
	const command = commandType.readValue(data)
	switch (command.type) {
		case 'list':
			return listReponseType.valueBuffer(await runList())
		case 'item_create':
			return voidReponseType.valueBuffer(await runItemCreate(command))
		case 'item_drop':
			return voidReponseType.valueBuffer(await runItemDrop(command))
		case 'item_get':
			return bytesResponseType.valueBuffer(await runItemGet(command))
		case 'item_set':
			return voidReponseType.valueBuffer(await runItemSet(command))
		case 'hash_create':
			return voidReponseType.valueBuffer(await runHashCreate(command))
		case 'hash_drop':
			return voidReponseType.valueBuffer(await runHashDrop(command))
		case 'hash_get':
			return optionalBytesResponseType.valueBuffer(await runHashGet(command))
		case 'hash_set':
			return voidReponseType.valueBuffer(await runHashSet(command))
		default:
			const unreachable: never = command
			unreachable
			console.error('Unrecognized command')
			return new ArrayBuffer(0)
	}
}