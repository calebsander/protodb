// Constructs a mapping of student IDs to student attributes
// and performs some lookups

const protobuf = require('protobufjs')
const {ProtoDBClient} = require('proto-database')

const client = new ProtoDBClient
const {root} = protobuf.parse(`
	syntax = "proto3";

	message KeyType {
		uint32 id = 1;
	}

	message ValueType {
		string firstName = 1;
		string lastName = 2;
		uint32 year = 3;
	}
`)
const keyType = root.lookupType('KeyType'),
      valueType = root.lookupType('ValueType')

async function main() {
	await client.hashCreate('students')
	console.log(await client.list())
	// { collections: { students: 1 } }

	const key1 = keyType.encode({id: 10452}).finish()
	const value1 = {firstName: 'Caleb', lastName: 'Sander', year: 2021}
	await client.hashSet('students', key1, valueType.encode(value1).finish())
	console.log(valueType.decode(await client.hashGet('students', key1)))
	// ValueType { firstName: 'Caleb', lastName: 'Sander', year: 2021 }
	console.log(await client.hashGet('students', keyType.encode({id: 10451}).finish()))
	// null

	const key2 = keyType.encode({id: 12345}).finish()
	const value2 = {firstName: 'Belac', lastName: 'Rednas', year: 3005}
	await client.hashSet('students', key2, valueType.encode(value2).finish())
	console.log(valueType.decode(await client.hashGet('students', key1)))
	// ValueType { firstName: 'Caleb', lastName: 'Sander', year: 2021 }
	console.log(valueType.decode(await client.hashGet('students', key2)))
	// ValueType { firstName: 'Belac', lastName: 'Rednas', year: 3005 }
	console.log(await client.hashGet('students', keyType.encode({id: 10451}).finish()))
	// null
}

main()