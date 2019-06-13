// Increments a count item 1000 times and reads the result

const protobuf = require('protobufjs')
const {ProtoDBClient} = require('proto-database')

const client = new ProtoDBClient
const countType = protobuf.parse(`
	syntax = "proto3";

	message Count {
		uint32 value = 1;
	}
`).root.lookupType('Count')

async function increment() {
	const data = await client.itemGet('count')
	const {value} = countType.toObject(countType.decode(data))
	await client.itemSet('count', countType.encode({value: value + 1}).finish())
}

async function main() {
	await client.itemCreate('count')
	await client.itemSet('count', countType.encode({value: 0}).finish())

	for (let i = 0; i < 1000; i++) await increment()
	console.log(countType.decode(await client.itemGet('count')))
	// Count { value: 1000 }
}

main()