import {Reader, Writer} from 'protobufjs'
import {Transform, TransformCallback, Writable} from 'stream'

const MAX_LENGTH_BYTES = new Writer().uint32(-1).len

interface ReadState {
	messageLength: number
	readLength: number
	readChunks: Buffer[]
}

export interface DelimitedReader {
	on(event: 'data', listener: (data: Buffer) => void): this
	on(event: 'end', listener: () => void): this
	on(event: string | symbol, listener: (...args: any[]) => void): this
}

export class DelimitedReader extends Writable {
	private unprocessedLength?: Buffer
	private readState?: ReadState

	_write(chunk: Buffer, _: string, callback: TransformCallback) {
		while (true) {
			if (this.readState) {
				const {messageLength, readLength, readChunks} = this.readState
				const remainingLength = messageLength - readLength
				if (remainingLength > chunk.length) {
					readChunks.push(chunk)
					this.readState.readLength += chunk.length
					break
				}
				else { // chunk contains rest of message
					readChunks.push(chunk.slice(0, remainingLength))
					chunk = chunk.slice(remainingLength)
					this.emit('data', Buffer.concat(readChunks))
					this.readState = undefined
				}
			}
			else {
				if (!chunk.length) break

				let previousLengthBytes: number
				let lengthBuffer: Buffer
				if (this.unprocessedLength) {
					previousLengthBytes = this.unprocessedLength.length
					lengthBuffer = Buffer.concat([
						this.unprocessedLength,
						chunk.slice(0, MAX_LENGTH_BYTES - previousLengthBytes)
					])
				}
				else {
					previousLengthBytes = 0
					lengthBuffer = chunk
				}
				try {
					const reader = new Reader(lengthBuffer)
					this.readState = {
						messageLength: reader.uint32(),
						readLength: 0,
						readChunks: []
					}
					chunk = chunk.slice(reader.pos - previousLengthBytes)
					this.unprocessedLength = undefined
				}
				catch {
					this.unprocessedLength = lengthBuffer
				}
			}
		}
		callback()
	}
	_final(callback: TransformCallback) {
		this.emit('end')
		callback(this.unprocessedLength || this.readState
			? new Error('Stream contains a partial message')
			: null
		)
	}
}

export class DelimitedWriter extends Transform {
	_transform(chunk: Buffer, _: string, callback: TransformCallback) {
		const {buffer, byteOffset, byteLength} =
			new Writer().uint32(chunk.length).finish()
		this.push(Buffer.from(buffer, byteOffset, byteLength))
		this.push(chunk)
		callback()
	}
}