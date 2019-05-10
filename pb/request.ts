import * as path from 'path'
import * as protobuf from 'protobufjs'
import {Type} from './common'
import {DB} from './db'

export const ITER_BYTE_LENGTH = 16

const protoFile = protobuf.loadSync(['request.proto', 'db.proto']
	.map(file => path.join(__dirname, file))
)

// These Uint8Arrays are optional because they may be empty:
// see https://github.com/protobufjs/protobuf.js/issues/1218
export interface NameParams {
	name: string
}
export interface NameValueParams extends NameParams {
	value?: Uint8Array
}
export interface NameKeyParams extends NameParams {
	key?: Uint8Array
}
export interface NameKeyValueParams extends NameKeyParams {
	value?: Uint8Array
}
export interface IterParams {
	iter: Uint8Array
}
export interface NameIndexParams extends NameParams {
	index?: number
}
export interface NameIndexValueParams extends NameIndexParams {
	value?: Uint8Array
}
export interface NameRangeParams extends NameParams {
	start?: number
	end?: number
}
export type Command
	= {list: {}}

	| {itemCreate: NameParams}
	| {itemDrop: NameParams}
	| {itemGet: NameParams}
	| {itemSet: NameValueParams}

	| {hashCreate: NameParams}
	| {hashDrop: NameParams}
	| {hashDelete: NameKeyParams}
	| {hashGet: NameKeyParams}
	| {hashSet: NameKeyValueParams}
	| {hashSize: NameParams}
	| {hashIter: NameParams}
	| {hashIterBreak: IterParams}
	| {hashIterNext: IterParams}

	| {listCreate: NameParams}
	| {listDrop: NameParams}
	| {listGet: NameIndexParams}
	| {listInsert: NameIndexValueParams}
	| {listSet: NameIndexValueParams}
export const commandType = protoFile.lookupType('Command') as Type<Command>

export interface ErrorResponse {
	error: string
}

export type BytesResponse = ErrorResponse | {data?: Uint8Array}
export const bytesResponseType =
	protoFile.lookupType('BytesResponse') as Type<BytesResponse>

export type IterResponse = ErrorResponse | {iter: Uint8Array}
export const iterResponseType =
	protoFile.lookupType('IterResponse') as Type<IterResponse>

export type ListResponse = ErrorResponse | {db: DB}
export const listResponseType =
	protoFile.lookupType('ListResponse') as Type<ListResponse>

// data can't be optional because of https://github.com/protobufjs/protobuf.js/issues/1218
export type OptionalBytesResponse = BytesResponse | {none: {}}
export const optionalBytesResponseType =
	protoFile.lookupType('OptionalBytesResponse') as Type<OptionalBytesResponse>

interface KeyValuePair {
	key: Uint8Array
	value: Uint8Array
}
export type OptionalPairResponse = ErrorResponse | {item?: KeyValuePair}
export const optionalPairResponseType =
	protoFile.lookupType('OptionalPairResponse') as Type<OptionalPairResponse>

export type SizeResponse = ErrorResponse | {size: number}
export const sizeResponseType =
	protoFile.lookupType('SizeResponse') as Type<SizeResponse>

export type VoidResponse = Partial<ErrorResponse>
export const voidResponseType =
	protoFile.lookupType('VoidResponse') as Type<VoidResponse>