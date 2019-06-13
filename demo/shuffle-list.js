// Creates a list containing a deck of cards and extracts them in a random order

const protobuf = require('protobufjs')
const {ProtoDBClient} = require('proto-database')

const client = new ProtoDBClient
const cardType = protobuf.parse(`
	syntax = "proto3";

	enum Suit {
		CLUBS = 0;
		DIAMONDS = 1;
		HEARTS = 2;
		SPADES = 3;
	}

	message Card {
		Suit suit = 1;
		uint32 number = 2;
	}
`).root.lookupType('Card')

async function main() {
	// Initialize deck
	await client.listCreate('deck')
	await Promise.all(new Array(4).fill().map((_, suit) =>
		Promise.all(new Array(13).fill().map((_, i) =>
			client.listInsert('deck', cardType.encode({suit, number: i + 1}).finish())
		))
	))
	console.log(await client.listSize('deck')) // 52

	for (let i = 52; i; i--) {
		const index = (Math.random() * i) | 0
		const data = await client.listGet('deck', index)
		console.log(cardType.toObject(cardType.decode(data), {enums: String}))
		await client.listDelete('deck', index)
	}
	/*
	{ suit: 'SPADES', number: 2 }
	{ suit: 'HEARTS', number: 2 }
	{ suit: 'DIAMONDS', number: 4 }
	...
	*/
}

main()