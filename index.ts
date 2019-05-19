import * as fs from 'fs'
import * as net from 'net'
import {promisify} from 'util'
import {dataDir, port} from './args'
import {shutdown} from './cache'
import {executeCommand} from './command-processor'
import {concat} from './util'

async function initDB(): Promise<void> {
	try {
		await promisify(fs.mkdir)(dataDir)
	}
	catch {} // not a problem if it already exists
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
			.on('end', async () =>
				connection.end(await executeCommand(concat(chunks)))
			)
	}).listen(port)
	console.log(`Listening on port ${port}`)
})()