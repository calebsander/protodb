import * as fs from 'fs'
import * as net from 'net'
import * as readline from 'readline'
import * as sb from 'structure-bytes'
import {promisify} from 'util'
import {PORT} from '../constants'
import {Command, commandType, listReponseType, voidReponseType} from '../sb-types/request'
import {concat, toArrayBuffer} from '../util'

const readFile = promisify(fs.readFile)

readline.createInterface(process.stdin)
	.on('line', async line => {
		const args = line.trim().split(/\s+/)
		let command: Command, responseType: sb.Type<any>
		try {
			const type = args[0].toLowerCase()
			switch (type) {
				case 'list': {
					command = {type}
					responseType = listReponseType
					break
				}
				case 'item_create': {
					const [name, typeFile] = args.slice(1) as (string | undefined)[]
					if (!(name && typeFile)) {
						throw new Error(`Syntax: ${type} name typeFile`)
					}
					const schema = toArrayBuffer(await readFile(typeFile))
					sb.r.type(schema) // check that the type can be read
					command = {type, name, schema}
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
			.on('end', () =>
				console.log(responseType.readValue(concat(responseChunks)))
			)
	})