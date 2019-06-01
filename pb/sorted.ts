import path from 'path'
import protobuf from 'protobufjs'
import {Type} from './common'
import {Key} from './interface'

const protoFile = protobuf.loadSync(path.join(__dirname, 'sorted.proto'))

export const LIST_END = 0
export interface FreePage {
	next: number
}
export const freePageType = protoFile.lookupType('FreePage') as Type<FreePage>

export interface Header {
	root: number
	size: number
	freePage: FreePage
}
export const headerType = protoFile.lookupType('Header') as Type<Header>

export interface InnerNode {
	keys: Key[]
	children: number[]
}
export interface LeafNode {
	keys: Key[]
	values: Uint8Array[]
	next: number
}
export type Node
	= {inner: InnerNode}
	| {leaf: LeafNode}
export const nodeType = protoFile.lookupType('Node') as Type<Node>