import * as sb from 'structure-bytes'

export const depthType = new sb.UnsignedByteType

interface BucketItem {
	key: ArrayBuffer
	value: ArrayBuffer
}
export interface Bucket {
	depth: number
	items: BucketItem[]
}

export const bucketType = new sb.StructType<Bucket>({
	depth: depthType,
	items: new sb.ArrayType(
		new sb.StructType({
			key: new sb.OctetsType,
			value: new sb.OctetsType
		})
	)
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