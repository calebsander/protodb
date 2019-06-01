import net = require('net')
import {DEFAULT_PORT} from '../constants'
import {Type} from '../pb/common'
import {DB, KeyValuePair} from '../pb/interface'
import {
	bytesResponseType,
	Command,
	commandType,
	ErrorResponse,
	iterResponseType,
	listResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	sizeResponseType,
	voidResponseType
} from '../pb/request'
import {concat} from '../util'

const toUint8Array = (buffer: ArrayBuffer | Uint8Array) =>
	buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer)
const toOptionalIndex = (index?: number) =>
	index === undefined ? {none: {}} : {value: index}
const toOptionalBytes = (value: {data: Uint8Array} | {none: {}}) =>
	'data' in value ? value.data : null

export class ProtoDBError extends Error {
	get name() {
		return this.constructor.name
	}
}

export class ProtoDBClient {
	constructor(
		public readonly port = DEFAULT_PORT,
		public readonly host = 'localhost'
	) {}

	private async runCommand<T extends object>(
		command: Command, responseType: Type<T | ErrorResponse>
	): Promise<T> {
		const client: net.Socket = net.connect(this.port, this.host, () =>
			client.end(commandType.encode(command).finish())
		)
		const data = await new Promise<Uint8Array>((resolve, reject) => {
			const chunks: Buffer[] = []
			client
				.on('data', chunk => chunks.push(chunk))
				.on('end', () => resolve(concat(chunks)))
				.on('error', reject)
		})
		const response = responseType.toObject(
			responseType.decode(data),
			{defaults: true, longs: Number}
		)
		if ('error' in response) throw new ProtoDBError(response.error)
		return response
	}

	async list(): Promise<DB> {
		const {db} = await this.runCommand({list: {}}, listResponseType)
		return db
	}
	async itemCreate(name: string): Promise<void> {
		await this.runCommand({itemCreate: {name}}, voidResponseType)
	}
	async itemDrop(name: string): Promise<void> {
		await this.runCommand({itemDrop: {name}}, voidResponseType)
	}
	async itemGet(name: string): Promise<Uint8Array> {
		const {data} = await this.runCommand({itemGet: {name}}, bytesResponseType)
		return data
	}
	async itemSet(name: string, value: ArrayBuffer | Uint8Array): Promise<void> {
		await this.runCommand(
			{itemSet: {name, value: toUint8Array(value)}},
			voidResponseType
		)
	}
	async hashCreate(name: string): Promise<void> {
		await this.runCommand({hashCreate: {name}}, voidResponseType)
	}
	async hashDrop(name: string): Promise<void> {
		await this.runCommand({hashDrop: {name}}, voidResponseType)
	}
	async hashDelete(name: string, key: ArrayBuffer | Uint8Array): Promise<void> {
		await this.runCommand(
			{hashDelete: {name, key: toUint8Array(key)}},
			optionalBytesResponseType
		)
	}
	async hashGet(name: string, key: ArrayBuffer | Uint8Array): Promise<Uint8Array | null> {
		const value = await this.runCommand(
			{hashGet: {name, key: toUint8Array(key)}},
			optionalBytesResponseType
		)
		return toOptionalBytes(value)
	}
	async hashSet(
		name: string, key: ArrayBuffer | Uint8Array, value: ArrayBuffer | Uint8Array
	): Promise<void> {
		await this.runCommand(
			{hashSet: {name, key: toUint8Array(key), value: toUint8Array(value)}},
			voidResponseType
		)
	}
	async hashSize(name: string): Promise<number> {
		const {size} = await this.runCommand({hashSize: {name}}, sizeResponseType)
		return size
	}
	async hashIter(name: string): Promise<Uint8Array> {
		const {iter} = await this.runCommand({hashIter: {name}}, iterResponseType)
		return iter
	}
	async hashIterBreak(iter: Uint8Array): Promise<void> {
		await this.runCommand({hashIterBreak: {iter}}, voidResponseType)
	}
	async hashIterNext(iter: Uint8Array): Promise<KeyValuePair | null> {
		const {item} = await this.runCommand(
			{hashIterNext: {iter}},
			optionalPairResponseType
		)
		return item || null
	}
	async listCreate(name: string): Promise<void> {
		await this.runCommand({listCreate: {name}}, voidResponseType)
	}
	async listDrop(name: string): Promise<void> {
		await this.runCommand({listDrop: {name}}, voidResponseType)
	}
	async listDelete(name: string, index?: number): Promise<void> {
		await this.runCommand(
			{listDelete: {name, index: toOptionalIndex(index)}},
			voidResponseType
		)
	}
	async listGet(name: string, index: number): Promise<Uint8Array> {
		const {data} =
			await this.runCommand({listGet: {name, index}}, bytesResponseType)
		return data
	}
	async listInsert(
		name: string, value: ArrayBuffer | Uint8Array, index?: number
	): Promise<void> {
		await this.runCommand(
			{listInsert: {
				name,
				index: toOptionalIndex(index),
				value: toUint8Array(value)
			}},
			voidResponseType
		)
	}
	async listSet(
		name: string, index: number, value: ArrayBuffer | Uint8Array
	): Promise<void> {
		await this.runCommand(
			{listSet: {name, index, value: toUint8Array(value)}},
			voidResponseType
		)
	}
	async listSize(name: string): Promise<number> {
		const {size} = await this.runCommand({listSize: {name}}, sizeResponseType)
		return size
	}
	async listIter(name: string, start?: number, end?: number): Promise<Uint8Array> {
		if (start && start < 0 || end && end < 0) {
			throw new RangeError(`Bounds cannot be end-relative; got ${start} and ${end}`)
		}
		const {iter} = await this.runCommand(
			{listIter: {
				name,
				start: toOptionalIndex(start),
				end: toOptionalIndex(end)
			}},
			iterResponseType
		)
		return iter
	}
	async listIterBreak(iter: Uint8Array): Promise<void> {
		await this.runCommand({listIterBreak: {iter}}, voidResponseType)
	}
	async listIterNext(iter: Uint8Array): Promise<Uint8Array | null> {
		const value = await this.runCommand(
			{listIterNext: {iter}},
			optionalBytesResponseType
		)
		return toOptionalBytes(value)
	}
}