// Fetches the schedules for Boston's Commuter Rail lines,
// inserts them into a sorted map, and runs a range query on them

const https = require('https')
const protobuf = require('protobufjs')
const {ProtoDBClient} = require('proto-database')

const TIME_ZONE = 'America/New_York'
const QUERY_STOP = 'South Station'

const queryAPI = url =>
	new Promise((resolve, reject) =>
		https.get(`https://api-v3.mbta.com/${url}`, res => {
			const chunks = []
			res
				.on('data', chunk => chunks.push(chunk))
				.on('end', () => resolve(JSON.parse(Buffer.concat(chunks))))
				.on('error', reject)
		})
	)

const client = new ProtoDBClient
const stopType = protobuf.parse(`
	syntax = "proto3";

	message Stop {
		uint32 trip = 1;
		string destination = 2;
	}
`).root.lookupType('Stop')

async function main() {
	await client.sortedCreate('stops')

	// Find all CR routes
	const {data: routes} = await queryAPI('routes?filter[type]=2')
	// Process the 26 routes at the same time since the insertion order isn't important
	await Promise.all(routes.map(async ({id}) => {
		// Find all trips and their stops
		const {data: stops, included: trips} =
			await queryAPI(`schedules?filter[route]=${id}&include=trip`)
		for (const {relationships, attributes} of stops) {
			const stopName = relationships.stop.data.id
			// Order stops by station and time
			const key = [
				{string: stopName},
				{int: Date.parse(attributes.departure_time) * 1e-3}
			]
			const tripId = relationships.trip.data.id
			const trip = trips.find(trip => trip.id === tripId).attributes
			const value = {trip: Number(trip.name), destination: trip.headsign}
			if (value.destination === stopName) continue // skip if this is the last stop

			await client.sortedInsert('stops', key, stopType.encode(value).finish())
		}
	}))

	// List all departures from the query stop, ordered by time
	const matchedTrips = await client.sortedGet('stops', [{string: QUERY_STOP}])
	for (const {key, value} of matchedTrips) {
		console.log(
			new Date(key[1].int * 1e3)
				.toLocaleTimeString('en-US', {timeZone: TIME_ZONE}),
			stopType.toObject(stopType.decode(value))
		)
	}
	/*
	3:50:00 AM { trip: 701, destination: 'Forge Park/495' }
	4:40:00 AM { trip: 501, destination: 'Worcester' }
	5:30:00 AM { trip: 583, destination: 'Framingham' }
	...
	*/
}

main()