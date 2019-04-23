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
	OptionalBytesResponse,
	bytesResponseType,
	listReponseType,
	optionalBytesResponseType,
	voidReponseType
} from '../sb-types/request'
import {concat} from '../util'

const readType = promisify(sb.readType)

readline.createInterface(process.stdin)
	.on('line', async line => {
		const args = line.trim().split(/\s+/)
		let command: Command, responseType: sb.Type<any>, bytesType: sb.Type<any>
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
					const [name, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile)) {
						throw new Error(`Syntax: ${type} name type_file`)
					}
					command = {type, name}
					responseType = bytesResponseType
					bytesType = await readType(fs.createReadStream(typeFile))
					break
				}
				case 'item_set': {
					const [name, typeFile, value] = args.slice(1) as (string | undefined)[]
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
					const [name, keyTypeFile, key, valueTypeFile] = args.slice(1) as (string | undefined)[]
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
					const [name, keyTypeFile, key, valueTypeFile, value] = args.slice(1) as (string | undefined)[]
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
				default:
					throw new Error(`Unrecognized command "${type}"`)
			}
		}
		catch (e) {
			console.error(e)
			return
		}

		const client = net.createConnection(PORT)
		const responseChunks: Buffer[] = []
		client
			.on('connect', () =>
				client.end(new Uint8Array(commandType.valueBuffer(command)))
			)
			.on('data', chunk => responseChunks.push(chunk))
			.on('end', () => {
				let response = responseType.readValue(concat(responseChunks))
				if (responseType === bytesResponseType) {
					const bytesResponse: BytesResponse = response
					if ('data' in bytesResponse) {
						response = bytesType.consumeValue(bytesResponse.data, 0).value
					}
				}
				else if (responseType === optionalBytesResponseType) {
					const bytesResponse: OptionalBytesResponse = response
					if ('data' in bytesResponse && bytesResponse.data) {
						response = bytesType.readValue(bytesResponse.data)
					}
				}
				console.log(response)
			})
	})