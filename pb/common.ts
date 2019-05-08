import * as protobuf from 'protobufjs'

export interface Type<T extends object> extends protobuf.Type {
	decode(reader: Uint8Array, length?: number): protobuf.Message<T>
	encode(message: protobuf.Message<T>): protobuf.Writer
	fromObject(value: T): protobuf.Message<T>
	toObject(message: protobuf.Message<T>): T
}