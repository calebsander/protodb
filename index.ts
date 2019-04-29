import * as fs from 'fs'
import * as net from 'net'
import {promisify} from 'util'
import {shutdown} from './cache'
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

async function cleanup(): Promise<void> {
	try {
		await shutdown()
	}
	catch (e) {
		console.error('Shutdown failed with error:', e)
	}
	process.exit()
}

(async () => {
	await initDB()
	process
		.on('exit', code => {
			if (code) console.error('Cache may not have been flushed')
		})
		.on('SIGTERM', cleanup)
		.on('SIGINT', cleanup)
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