import * as sb from 'structure-bytes'

export interface Item {
	value: ArrayBuffer | null
}
export const itemValueType = new sb.StructType<Item>({
	value: new sb.OptionalType(new sb.OctetsType)
})