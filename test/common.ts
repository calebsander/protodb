import {exec, spawn} from 'child_process'
import path = require('path')
import readline = require('readline')
import {promisify} from 'util'
import {ProtoDBClient} from '../client'
import {DEFAULT_PORT} from '../constants'

const DB_PATH = 'dist/main.js'

const execPromise = promisify(exec)

export class TestContext {
	private static testIndex = 0

	private readonly index = TestContext.testIndex++
	private readonly db = spawn(DB_PATH, ['-p', `${this.port}`, '-d', this.dataDir])
	public readonly ready = new Promise<void>((resolve, reject) => {
		readline.createInterface(this.db.stdout)
			.on('line', line => {
				if (line.startsWith('Listening')) resolve()
			})
			.on('error', reject)
	})
	private readonly closed = new Promise<void>(resolve =>
		this.db.on('close', resolve)
	)
	public readonly client = new ProtoDBClient(this.port)

	private get port() {
		return DEFAULT_PORT + this.index
	}
	private get dataDir() {
		return `test${this.index}-data`
	}
	async close(): Promise<void> {
		this.db.kill()
		await this.closed
		await execPromise(`rm -rf ${this.dataDir}`)
	}
	getFile(filename: string): string {
		return path.join(this.dataDir, filename)
	}
}