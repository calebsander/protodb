import * as sb from 'structure-bytes'

export const literalType = <E extends string>(value: E) =>
	new sb.SingletonType<E>({type: new sb.StringType, value})

export type Schema = ArrayBuffer
export const schemaType: sb.Type<Schema> = new sb.OctetsType