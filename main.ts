#!/usr/bin/env node

import {promises as fs} from 'fs'
import net = require('net')
import {dataDir, port} from './args'
import {shutdown} from './cache'
import {processCommand} from './command-processor'
import {DelimitedReader, DelimitedWriter} from './delimited-stream'

const initDB = (): Promise<void> =>
	fs.mkdir(dataDir).catch(_ => {}) // not a problem if it already exists

async function cleanup(): Promise<void> {
	try {
		await shutdown()
	}
	catch (e) {
		// istanbul ignore next
		console.error('Shutdown failed with error:', e)
	}
	process.exit()
}

(async () => {
	await initDB()
	process
		.on('exit', code => {
			// istanbul ignore if
			if (code) console.error('Cache may not have been flushed')
		})
		.on('SIGTERM', cleanup)
		.on('SIGINT', cleanup)
	net.createServer(connection => {
		const responseStream = new DelimitedWriter
		responseStream.pipe(connection)
		connection.pipe(new DelimitedReader)
			.on('data', (command: Buffer) =>
				processCommand(command, response => responseStream.write(response))
			)
	}).listen(port)
	console.log(`Listening on port ${port}`)
})()