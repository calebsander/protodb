import path = require('path')
import protobuf = require('protobufjs')
import {Type} from './common'
import {KeyValuePair} from './interface'

const protoFile = protobuf.loadSync(
	['hash.proto', 'interface.proto'].map(file => path.join(__dirname, file))
)

export interface Header {
	depth: number
	size: number
}
export const headerType = protoFile.lookupType('Header') as Type<Header>
// Header and bucket index have constant size
// for constant-time access into the directory array
export const HEADER_BYTES = headerType.encode({depth: 0, size: 0}).len

export interface BucketIndex {
	page: number
}
export const bucketIndexType =
	protoFile.lookupType('BucketIndex') as Type<BucketIndex>
export const BUCKET_INDEX_BYTES = bucketIndexType.encode({page: 0}).len

export const bucketItemType =
	protoFile.lookupType('KeyValuePair') as Type<KeyValuePair>

export interface Bucket {
	depth: number
	items: KeyValuePair[]
}
export const bucketType = protoFile.lookupType('Bucket') as Type<Bucket>