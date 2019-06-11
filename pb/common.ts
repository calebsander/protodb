import protobuf = require('protobufjs')

// A typed version of the protobuf interface for better static type checking
export interface Type<T extends object> extends protobuf.Type {
	decode(reader: Uint8Array): protobuf.Message<T>
	decodeDelimited(reader: Uint8Array): protobuf.Message<T>
	encode(message: T): protobuf.Writer
	encodeDelimited(message: T): protobuf.Writer
	toObject(message: protobuf.Message<T>, options?: protobuf.IConversionOptions): T
}