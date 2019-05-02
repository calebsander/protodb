import * as fs from 'fs'
import * as net from 'net'
import * as readline from 'readline'
import * as sb from 'structure-bytes'
import {promisify} from 'util'
import {PORT} from '../constants'
import {
	Command,
	commandType,
	BytesResponse,
	IterResponse,
	OptionalBytesResponse,
	OptionalPairResponse,
	bytesResponseType,
	iterResponseType,
	listReponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	unsignedResponseType,
	voidReponseType
} from '../sb-types/request'
import {concat} from '../util'

const readType = promisify(sb.readType)

const toHexString = (bytes: number[]): string =>
	bytes.map(b => (b < 16 ? '0' : '') + b.toString(16)).join('')
function fromHexString(str: string): number[] {
	const bytes = new Array<number>(str.length >> 1)
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = parseInt(str.substr(i << 1, 2), 16)
	}
	return bytes
}

async function processCommands() {
	const rl = readline.createInterface(process.stdin)
	for await (const line of rl) {
		const args = line.trim().split(/\s+/)
		let command: Command
		let responseType: sb.Type<any>, bytesType: sb.Type<any>
		let keyType: sb.Type<any>, valueType: sb.Type<any>
		try {
			const type = args[0].toLowerCase()
			switch (type) {
				case 'list': {
					command = {type}
					responseType = listReponseType
					break
				}
				case 'item_create': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = voidReponseType
					break
				}
				case 'item_drop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = voidReponseType
					break
				}
				case 'item_get': {
					const [name, typeFile]: (string | undefined)[] = args.slice(1)
					if (!(name && typeFile)) {
						throw new Error(`Syntax: ${type} name type_file`)
					}
					command = {type, name}
					responseType = bytesResponseType
					bytesType = await readType(fs.createReadStream(typeFile))
					break
				}
				case 'item_set': {
					const [name, typeFile, value]: (string | undefined)[] = args.slice(1)
					if (!(name && typeFile && value)) {
						throw new Error(`Syntax: ${type} name type_file value`)
					}
					const valueType = await readType(fs.createReadStream(typeFile))
					command = {type, name, value: valueType.valueBuffer(JSON.parse(value))}
					responseType = voidReponseType
					break
				}
				case 'hash_create': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = voidReponseType
					break
				}
				case 'hash_drop': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = voidReponseType
					break
				}
				case 'hash_get': {
					const [name, keyTypeFile, key, valueTypeFile]: (string | undefined)[] = args.slice(1)
					if (!(name && keyTypeFile && key && valueTypeFile)) {
						throw new Error(`Syntax: ${type} name key_type_file key value_type_file`)
					}
					const [keyType, valueType] = await Promise.all(
						[keyTypeFile, valueTypeFile].map(typeFile =>
							readType(fs.createReadStream(typeFile))
						)
					)
					command = {type, name, key: keyType.valueBuffer(JSON.parse(key))}
					responseType = optionalBytesResponseType
					bytesType = valueType
					break
				}
				case 'hash_set': {
					const [name, keyTypeFile, key, valueTypeFile, value]: (string | undefined)[] = args.slice(1)
					if (!(name && keyTypeFile && key && valueTypeFile && value)) {
						throw new Error(`Syntax: ${type} name key_type_file key value_type_file value`)
					}
					const [keyType, valueType] = await Promise.all(
						[keyTypeFile, valueTypeFile].map(typeFile =>
							readType(fs.createReadStream(typeFile))
						)
					)
					command = {
						type,
						name,
						key: keyType.valueBuffer(JSON.parse(key)),
						value: valueType.valueBuffer(JSON.parse(value))
					}
					responseType = voidReponseType
					break
				}
				case 'hash_delete': {
					const [name, keyTypeFile, key]: (string | undefined)[] = args.slice(1)
					if (!(name && keyTypeFile && key)) {
						throw new Error(`Syntax: ${type} name key_type_file key`)
					}
					const keyType = await readType(fs.createReadStream(keyTypeFile))
					command = {type, name, key: keyType.valueBuffer(JSON.parse(key))}
					responseType = voidReponseType
					break
				}
				case 'hash_size': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = unsignedResponseType
					break
				}
				case 'hash_iter': {
					const name: string | undefined = args[1]
					if (!name) throw new Error(`Syntax: ${type} name`)
					command = {type, name}
					responseType = iterResponseType
					break
				}
				case 'hash_iter_next': {
					const [iter, keyTypeFile, valueTypeFile]: (string | undefined)[] = args.slice(1)
					if (!(iter && keyTypeFile && valueTypeFile)) {
						throw new Error(`Syntax: ${type} iter key_type_file value_type_file`)
					}
					command = {type, iter: fromHexString(iter)}
					responseType = optionalPairResponseType
					;[keyType, valueType] = await Promise.all(
						[keyTypeFile, valueTypeFile].map(typeFile =>
							readType(fs.createReadStream(typeFile))
						)
					)
					break
				}
				case 'hash_iter_break': {
					const iter: string | undefined = args[1]
					if (!iter) throw new Error(`Syntax: ${type} iter`)
					command = {type, iter: fromHexString(iter)}
					responseType = voidReponseType
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

		const client: net.Socket = net.createConnection(PORT)
			.on('connect', () =>
				client.end(new Uint8Array(commandType.valueBuffer(command)))
			)
		const response = await new Promise((resolve, reject) => {
			const responseChunks: Buffer[] = []
			client
				.on('data', chunk => responseChunks.push(chunk))
				.on('end', () => {
					let response = responseType.readValue(concat(responseChunks))
					switch (responseType) {
						case bytesResponseType: {
							const bytesResponse: BytesResponse = response
							if ('data' in bytesResponse) {
								response = bytesType.readValue(bytesResponse.data)
							}
							break
						}
						case iterResponseType:
							const iterResponse: IterResponse = response
							if ('iter' in iterResponse) {
								response = toHexString(iterResponse.iter)
							}
							break
						case optionalBytesResponseType: {
							const bytesResponse: OptionalBytesResponse = response
							if ('data' in bytesResponse && bytesResponse.data) {
								response = bytesType.readValue(bytesResponse.data)
							}
							break
						}
						case optionalPairResponseType:
							const pairResponse: OptionalPairResponse = response
							if ('item' in pairResponse && pairResponse.item) {
								const {key, value} = pairResponse.item
								response = {
									key: keyType.readValue(key),
									value: valueType.readValue(value)
								}
							}
					}
					resolve(response)
				})
				.on('error', reject)
		})
		console.log(response)
	}
}

processCommands()