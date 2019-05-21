import net from 'net'
import readline from 'readline'
import {inspect} from 'util'
import protobuf from 'protobufjs'
import yargs from 'yargs'
import {DEFAULT_PORT} from '../constants'
import {Type} from '../pb/common'
import {
	Command,
	commandType,
	IterResponse,
	OptionalBytesResponse,
	OptionalPairResponse,
	bytesResponseType,
	iterResponseType,
	listResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	sizeResponseType,
	voidResponseType
} from '../pb/request'
import {concat} from '../util'

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

async function lookupType(file: string, ...types: string[]): Promise<Type<any>[]> {
	const protoFile = await protobuf.load(file)
	return types.map(type => protoFile.lookupType(type) as Type<any>)
}

async function processCommands() {
	const rl = readline.createInterface(process.stdin)
	for await (const line of rl) {
		const trimmedLine = line.trim()
		if (!trimmedLine) continue

		const args = trimmedLine.split(/\s+/)
		let command: Command
		let responseType: Type<any>
		let bytesType: Type<any>, keyType: Type<any>, valueType: Type<any>
		try {
			const [type] = args
			switch (type) {
				case 'list':
					command = {[type]: {}}
					responseType = listResponseType
					break
				case 'itemCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'itemDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'itemGet': {
					const [name, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile)) {
						throw new Error(`Syntax: ${type} name typeFile`)
					}
					[bytesType] = await lookupType(typeFile, 'Type')
					command = {[type]: {name}}
					responseType = bytesResponseType
					break
				}
				case 'itemSet': {
					const [name, typeFile, value] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && value)) {
						throw new Error(`Syntax: ${type} name typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					command = {[type]: {
						name,
						value: valueType.encode(valueType.fromObject(JSON.parse(value))).finish()
					}}
					responseType = voidResponseType
					break
				}
				case 'hashCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'hashDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'hashDelete': {
					const [name, typeFile, key] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key)) {
						throw new Error(`Syntax: ${type} name typeFile key`)
					}
					const [keyType] = await lookupType(typeFile, 'KeyType')
					command = {[type]: {
						name,
						key: keyType.encode(keyType.fromObject(JSON.parse(key))).finish()
					}}
					responseType = voidResponseType
					break
				}
				case 'hashGet': {
					const [name, typeFile, key] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key)) {
						throw new Error(`Syntax: ${type} name typeFile key`)
					}
					const [keyType, valueType] =
						await lookupType(typeFile, 'KeyType', 'ValueType')
					command = {[type]: {
						name,
						key: keyType.encode(keyType.fromObject(JSON.parse(key))).finish()
					}}
					responseType = optionalBytesResponseType
					bytesType = valueType
					break
				}
				case 'hashSet': {
					const [name, typeFile, key, value] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile && key && value)) {
						throw new Error(`Syntax: ${type} name typeFile key value`)
					}
					const [keyType, valueType] =
						await lookupType(typeFile, 'KeyType', 'ValueType')
					command = {[type]: {
						name,
						key: keyType.encode(keyType.fromObject(JSON.parse(key))).finish(),
						value: valueType.encode(valueType.fromObject(JSON.parse(value))).finish()
					}}
					responseType = voidResponseType
					break
				}
				case 'hashSize': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = sizeResponseType
					break
				}
				case 'hashIter': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = iterResponseType
					break
				}
				case 'hashIterBreak': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					command = {[type]: {iter: fromHexString(iter)}}
					responseType = voidResponseType
					break
				}
				case 'hashIterNext': {
					const [iter, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(iter && typeFile)) {
						throw new Error(`Syntax: ${type} iter typeFile`)
					}
					[keyType, valueType] = await lookupType(typeFile, 'KeyType', 'ValueType')
					command = {[type]: {iter: fromHexString(iter)}}
					responseType = optionalPairResponseType
					break
				}
				case 'listCreate': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'listDrop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = voidResponseType
					break
				}
				case 'listDelete': {
					const [name, index] = args.slice(1) as (string | undefined)[]
					if (!name) throw new Error(`Syntax: ${type} name [index]`)
					command = {[type]: {
						name,
						index: index === undefined ? {none: {}} : {value: Number(index)}
					}}
					responseType = voidResponseType
					break
				}
				case 'listGet': {
					const [name, index, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && index && typeFile)) {
						throw new Error(`Syntax: ${type} name index typeFile`)
					}
					[bytesType] = await lookupType(typeFile, 'Type')
					command = {[type]: {name, index: Number(index)}}
					responseType = bytesResponseType
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
					command = {[type]: {
						name,
						index: index === undefined ? {none: {}} : {value: Number(index)},
						value: valueType.encode(valueType.fromObject(JSON.parse(value))).finish()
					}}
					responseType = voidResponseType
					break
				}
				case 'listSet': {
					const [name, index, typeFile, value] = args.slice(1) as (string | undefined)[]
					if (!(name && index && typeFile && value)) {
						throw new Error(`Syntax: ${type} name index typeFile value`)
					}
					const [valueType] = await lookupType(typeFile, 'Type')
					command = {[type]: {
						name,
						index: Number(index),
						value: valueType.encode(valueType.fromObject(JSON.parse(value))).finish()
					}}
					responseType = voidResponseType
					break
				}
				case 'listSize': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {[type]: {name}}
					responseType = sizeResponseType
					break
				}
				case 'listIter': {
					const [name, start, end] = args.slice(1) as (string | undefined)[]
					if (!name) throw new Error(`Syntax: ${type} name [start [end]]`)
					command = {[type]: {
						name,
						start: start === undefined ? {none: {}} : {value: Number(start)},
						end: end === undefined ? {none: {}} : {value: Number(end)}
					}}
					responseType = iterResponseType
					break
				}
				case 'listIterBreak': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					command = {[type]: {iter: fromHexString(iter)}}
					responseType = voidResponseType
					break
				}
				case 'listIterNext': {
					const [iter, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(iter && typeFile)) {
						throw new Error(`Syntax: ${type} iter typeFile`)
					}
					[bytesType] = await lookupType(typeFile, 'Type')
					command = {[type]: {iter: fromHexString(iter)}}
					responseType = optionalBytesResponseType
					break
				}
				default:
					throw new Error(`Unrecognized command "${type}"`)
			}
		}
		catch (e) {
			console.error(e)
			continue
		}

		const client: net.Socket = net.connect(port)
			.on('connect', () => client.end(commandType.encode(command).finish()))
		const response = await new Promise((resolve, reject) => {
			const responseChunks: Buffer[] = []
			client
				.on('data', chunk => responseChunks.push(chunk))
				.on('end', () => {
					let response = responseType.toObject(
						responseType.decode(concat(responseChunks)),
						{defaults: true, longs: Number}
					)
					switch (responseType) {
						case bytesResponseType:
						case optionalBytesResponseType:
							const bytesResponse: OptionalBytesResponse = response
							if (!('none' in bytesResponse) && 'data' in bytesResponse) {
								response = bytesType.toObject(
									bytesType.decode(bytesResponse.data || new Uint8Array)
								)
							}
							break
						case iterResponseType:
							const iterResponse: IterResponse = response
							if ('iter' in iterResponse) {
								response = toHexString(iterResponse.iter)
							}
							break
						case optionalPairResponseType:
							const pairResponse: OptionalPairResponse = response
							if ('item' in pairResponse && pairResponse.item) {
								const {key, value} = pairResponse.item
								response = {
									key: keyType.toObject(keyType.decode(key)),
									value: valueType.toObject(valueType.decode(value))
								}
							}
					}
					resolve(response)
				})
				.on('error', reject)
		})
		console.log(inspect(response, {depth: Infinity, colors: true}))
	}
}

processCommands()