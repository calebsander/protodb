import {Transform, TransformCallback, Writable} from 'stream'
import {Reader, Writer} from 'protobufjs'

/*
	Implements a protocol that allows multiple messages to be written in sequence
	to a stream (e.g. a TCP socket).
	Each message is prepended with its length in bytes as a varint.
*/

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

// Really a transform stream, but it needs to be able to emit empty buffers
export class DelimitedReader extends Writable {
	private unprocessedLength?: Buffer
	private readState?: ReadState

	_write(chunk: Buffer, _: string, callback: TransformCallback) {
		// Process the chunk until it has been fully consumed
		while (true) {
			if (this.readState) { // in the middle of reading a message
				const {messageLength, readLength, readChunks} = this.readState
				// Check how many additional bytes are needed
				const remainingLength = messageLength - readLength
				if (chunk.length < remainingLength) { // chunk contains part of message
					readChunks.push(chunk)
					this.readState.readLength += chunk.length
					break
				}
				else { // chunk contains entire rest of message
					readChunks.push(chunk.slice(0, remainingLength))
					chunk = chunk.slice(remainingLength)
					this.emit('data', Buffer.concat(readChunks))
					this.readState = undefined // back to reading a message length
				}
			}
			else { // in the middle of reading a message length
				if (!chunk.length) break

				// Concatenate any previous length bytes with the chunk
				let previousLengthBytes: number
				if (this.unprocessedLength) {
					previousLengthBytes = this.unprocessedLength.length
					this.unprocessedLength = Buffer.concat([
						this.unprocessedLength,
						chunk.slice(0, MAX_LENGTH_BYTES - previousLengthBytes)
					])
				}
				else {
					previousLengthBytes = 0
					this.unprocessedLength = chunk
				}

				// Try to read a varint
				try {
					const reader = new Reader(this.unprocessedLength)
					this.readState = {
						messageLength: reader.uint32(),
						readLength: 0,
						readChunks: []
					}
					// Read succeeded, so the rest of the chunk is part of the message
					chunk = chunk.slice(reader.pos - previousLengthBytes)
					this.unprocessedLength = undefined
				}
				catch {
					break // need to wait for next chunk to get message length
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
		// Concatenate buffers so they are not issued as separate TCP packets
		callback(null, Buffer.concat([
			new Writer().uint32(chunk.length).finish(),
			chunk
		]))
	}
}