import * as path from 'path'
import * as protobuf from 'protobufjs'
import {Type} from './common'

const protoFile = protobuf.loadSync(path.join(__dirname, 'hash.proto'))

export interface Header {
	depth: number
	size: number
}
export const headerType = protoFile.lookupType('Header') as Type<Header>
export const HEADER_BYTES =
	headerType.encode(headerType.fromObject({depth: 0, size: 0}))
		.finish()
		.length

export interface BucketIndex {
	page: number
}
export const bucketIndexType =
	protoFile.lookupType('BucketIndex') as Type<BucketIndex>
export const BUCKET_INDEX_BYTES =
	bucketIndexType.encode(bucketIndexType.fromObject({page: 0}))
		.finish()
		.length

export interface BucketItem {
	key: Uint8Array
	value: Uint8Array
}
export const bucketItemType =
	protoFile.lookupType('BucketItem') as Type<BucketItem>

export interface Bucket {
	depth: number
	items: BucketItem[]
}
export const bucketType = protoFile.lookupType('Bucket') as Type<Bucket>