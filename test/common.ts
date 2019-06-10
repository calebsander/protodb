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
	private db = this.newDB
	public readonly client = new ProtoDBClient(this.port)

	private get port() {
		return DEFAULT_PORT + this.index
	}
	private get dataDir() {
		return `test${this.index}-data`
	}
	private get newDB() {
		return spawn(DB_PATH, ['-p', `${this.port}`, '-d', this.dataDir])
	}
	private get closed() {
		return new Promise<void>(resolve => this.db.on('close', resolve))
	}
	get ready() {
		return new Promise<void>((resolve, reject) => {
			readline.createInterface(this.db.stdout)
				.on('line', line => {
					if (line.startsWith('Listening')) resolve()
				})
				.on('error', reject)
		})
	}

	async close(): Promise<void> {
		this.db.kill()
		await this.closed
		await execPromise(`rm -rf ${this.dataDir}`)
	}
	async restart(): Promise<void> {
		this.db.kill()
		await this.closed
		this.db = this.newDB
		await this.ready
	}
	getFile(filename: string): string {
		return path.join(this.dataDir, filename)
	}
}