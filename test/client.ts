import readline = require('readline')
import protobuf = require('protobufjs')
import yargs = require('yargs')
import {ProtoDBClient} from '../client'
import {DEFAULT_PORT} from '../constants'

const {port} = yargs.options({
	port: {
		default: DEFAULT_PORT,
		number: true,
		describe: 'Port that protoDB is listening on'
	}
}).argv

const toHexString = (bytes: Uint8Array): string =>
	[...bytes].map(b => (b < 16 ? '0' : '') + b.toString(16)).join('')
function fromHexString(str: string): Uint8Array {
	const bytes = new Uint8Array(str.length >> 1)
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = parseInt(str.substr(i << 1, 2), 16)
	}
	return bytes
}

type Types<T> = {
	[k in keyof T]: protobuf.Type
}

async function lookupType<T extends string[]>(file: string, ...types: T): Promise<Types<T>> {
	const protoFile = await protobuf.load(file)
	return types.map(type => protoFile.lookupType(type)) as Types<T>
}

async function processCommands() {
	const client = new ProtoDBClient(port)
	const rl = readline.createInterface(process.stdin)
	for await (const line of rl) {
		const trimmedLine = line.trim()
		if (!trimmedLine) continue

		const args = trimmedLine.split(/\s+/)
		try {
			const [type] = args
			switch (type) {
				case 'list':
					console.log(await client.list())
					break
				// Name-only commands
				case 'itemCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.itemCreate(name)
					break
				}
				case 'itemDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.itemDrop(name)
					break
				}
				case 'itemGet': {
					const [name, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile)) {
						throw new Error(`Syntax: ${type} name typeFile`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					const result = await client.itemGet(name)
					console.log(valueType.toObject(valueType.decode(result)))
					break
				}
				case 'itemSet': {
					const [name, typeFile, value] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && value)) {
						throw new Error(`Syntax: ${type} name typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					await client.itemSet(name, valueType.encode(JSON.parse(value)).finish())
					break
				}
				case 'hashCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.hashCreate(name)
					break
				}
				case 'hashDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.hashDrop(name)
					break
				}
				case 'hashDelete': {
					const [name, typeFile, key] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key)) {
						throw new Error(`Syntax: ${type} name typeFile key`)
					}
					const [keyType] = await lookupType(typeFile, 'KeyType')
					await client.hashDelete(name, keyType.encode(JSON.parse(key)).finish())
					break
				}
				case 'hashGet': {
					const [name, typeFile, key] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key)) {
						throw new Error(`Syntax: ${type} name typeFile key`)
					}
					const [keyType, valueType] =
						await lookupType(typeFile, 'KeyType', 'ValueType')
					const result =
						await client.hashGet(name, keyType.encode(JSON.parse(key)).finish())
					console.log(result && valueType.toObject(valueType.decode(result)))
					break
				}
				case 'hashSet': {
					const [name, typeFile, key, value] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key && value)) {
						throw new Error(`Syntax: ${type} name typeFile key value`)
					}
					const [keyType, valueType] =
						await lookupType(typeFile, 'KeyType', 'ValueType')
					await client.hashSet(
						name,
						keyType.encode(JSON.parse(key)).finish(),
						valueType.encode(JSON.parse(value)).finish()
					)
					break
				}
				case 'hashSize': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					console.log(await client.hashSize(name))
					break
				}
				case 'hashIter': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					const iter = await client.hashIter(name)
					console.log(toHexString(iter))
					break
				}
				case 'hashIterBreak': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					await client.hashIterBreak(fromHexString(iter))
					break
				}
				case 'hashIterNext': {
					const [iter, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(iter && typeFile)) {
						throw new Error(`Syntax: ${type} iter typeFile`)
					}
					const [keyType, valueType] =
						await lookupType(typeFile, 'KeyType', 'ValueType')
					const result = await client.hashIterNext(fromHexString(iter))
					console.log(result && {
						key: keyType.toObject(keyType.decode(result.key)),
						value: valueType.toObject(valueType.decode(result.value))
					})
					break
				}
				case 'listCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.listCreate(name)
					break
				}
				case 'listDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.listDrop(name)
					break
				}
				case 'listDelete': {
					const [name, index] = args.slice(1) as (string | undefined)[]
					if (!(name && index)) throw new Error(`Syntax: ${type} name [index]`)
					await client.listDelete(name, Number(index))
					break
				}
				case 'listGet': {
					const [name, index, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && index && typeFile)) {
						throw new Error(`Syntax: ${type} name index typeFile`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					const result = await client.listGet(name, Number(index))
					console.log(valueType.toObject(valueType.decode(result)))
					break
				}
				case 'listInsert': {
					const insertArguments = args.slice(1)
					let name: string, index: string | undefined, typeFile: string, value: string
					switch (insertArguments.length) {
						case 3:
							[name, typeFile, value] = insertArguments
							break
						case 4:
							[name, index, typeFile, value] = insertArguments
							break
						default:
							throw new Error(`Syntax: ${type} name [index] typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					await client.listInsert(
						name,
						valueType.encode(JSON.parse(value)).finish(),
						index ? Number(index) : undefined
					)
					break
				}
				case 'listSet': {
					const [name, index, typeFile, value] = args.slice(1) as (string | undefined)[]
					if (!(name && index && typeFile && value)) {
						throw new Error(`Syntax: ${type} name index typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					await client.listSet(
						name,
						Number(index),
						valueType.encode(JSON.parse(value)).finish()
					)
					break
				}
				case 'listSize': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					console.log(await client.listSize(name))
					break
				}
				case 'listIter': {
					const [name, start, end] = args.slice(1) as (string | undefined)[]
					if (!name) throw new Error(`Syntax: ${type} name [start [end]]`)
					const iter = await client.listIter(
						name,
						start ? Number(start) : undefined,
						end ? Number(end) : undefined
					)
					console.log(toHexString(iter))
					break
				}
				case 'listIterBreak': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					await client.listIterBreak(fromHexString(iter))
					break
				}
				case 'listIterNext': {
					const [iter, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(iter && typeFile)) {
						throw new Error(`Syntax: ${type} iter typeFile`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					const result = await client.listIterNext(fromHexString(iter))
					console.log(result && valueType.toObject(valueType.decode(result)))
					break
				}
				case 'sortedCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.sortedCreate(name)
					break
				}
				case 'sortedDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					await client.sortedDrop(name)
					break
				}
				case 'sortedDelete': {
					const [name, key] = args.slice(1) as (string | undefined)[]
					if (!(name && key)) {
						throw new Error(`Syntax: ${type} name key`)
					}
					await client.sortedDelete(name, JSON.parse(key))
					break
				}
				case 'sortedGet': {
					const [name, key, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && key && typeFile)) {
						throw new Error(`Syntax: ${type} name key typeFile`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					const result = await client.sortedGet(name, JSON.parse(key))
					console.log(result.map(({key, value}) => ({
						key,
						value: valueType.toObject(valueType.decode(value))
					})))
					break
				}
				case 'sortedInsert': {
					const [name, key, typeFile, value] = args.slice(1) as (string | undefined)[]
					if (!(name && key && typeFile && value)) {
						throw new Error(`Syntax: ${type} name key typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					await client.sortedInsert(
						name,
						JSON.parse(key),
						valueType.encode(JSON.parse(value)).finish()
					)
					break
				}
				case 'sortedSize': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					console.log(await client.sortedSize(name))
					break
				}
				case 'sortedIter': {
					const [name, start, end, inclusive] = args.slice(1) as (string | undefined)[]
					if (!name) throw new Error(`Syntax: ${type} name [start [end ["in"]]]`)
					const iter = await client.sortedIter(
						name,
						start ? JSON.parse(start) : undefined,
						end ? JSON.parse(end) : undefined,
						!!inclusive
					)
					console.log(toHexString(iter))
					break
				}
				case 'sortedIterBreak': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					await client.sortedIterBreak(fromHexString(iter))
					break
				}
				case 'sortedIterNext': {
					const [iter, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(iter && typeFile)) {
						throw new Error(`Syntax: ${type} iter typeFile`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					const result = await client.sortedIterNext(fromHexString(iter))
					console.log(result && {
						key: result.key,
						value: valueType.toObject(valueType.decode(result.value))
					})
					break
				}
				default:
					throw new Error(`Unrecognized command "${type}"`)
			}
		}
		catch (e) {
			console.error(e)
		}
	}
	await client.close()
}

processCommands()