import * as fs from 'fs'
import * as net from 'net'
import {promisify} from 'util'
import {getCollections} from './collections'
import {runCommand} from './command-processor'
import {DATA_DIR, PORT} from './constants'
import {concat} from './util'

async function initDB(): Promise<void> {
	try {
		await promisify(fs.mkdir)(DATA_DIR)
	}
	catch {} // not a problem if it already exists
	await getCollections()
}

(async () => {
	await initDB()
	net.createServer({allowHalfOpen: true}, connection => {
		const chunks: Buffer[] = []
		connection
			.on('data', chunk => chunks.push(chunk))
			.on('end', async () => {
				const response = await runCommand(concat(chunks))
				connection.end(new Uint8Array(response))
			})
	}).listen(PORT)
	console.log(`Listening on port ${PORT}`)
})()