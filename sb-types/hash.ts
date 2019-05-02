import * as sb from 'structure-bytes'

const depthType = new sb.UnsignedByteType

export interface Header {
	depth: number
	size: number
}
export const headerType = new sb.StructType<Header>({
	depth: depthType,
	size: new sb.FlexUnsignedIntType
})

export interface BucketItem {
	key: ArrayBuffer
	value: ArrayBuffer
}
export const bucketItemType = new sb.StructType<BucketItem>({
	key: new sb.OctetsType,
	value: new sb.OctetsType
})

export interface Bucket {
	depth: number
	items: BucketItem[]
}
export const bucketType = new sb.StructType<Bucket>({
	depth: depthType,
	items: new sb.ArrayType(bucketItemType)
})

/*
Layout of directory file:
depth (depthType)
Padding until the end of the first page
bucketIndex[2 ** depth] (bucketIndexType)
*/

export const bucketIndexType = new sb.UnsignedIntType
// The size of a bucket index
export const BUCKET_INDEX_BYTES = bucketIndexType.valueBuffer(0).byteLength