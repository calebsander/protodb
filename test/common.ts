import {ChildProcess, exec, spawn} from 'child_process'
import net from 'net'
import path from 'path'
import readline from 'readline'
import {promisify} from 'util'
import {DEFAULT_PORT} from '../constants'
import {Type} from '../pb/common'
import {Command, commandType} from '../pb/request'
import {concat} from '../util'

const DB_PATH = 'dist/main.js'

const execPromise = promisify(exec)

// TODO: expose a ProtoDBClient
export class TestContext {
	private static testIndex = 0

	private readonly index: number
	private readonly db: ChildProcess
	private readonly ready: Promise<void>
	private readonly closed: Promise<void>

	constructor() {
		this.index = TestContext.testIndex++
		this.db = spawn(DB_PATH, ['-p', `${this.port}`, '-d', this.dataDir])
		this.ready = new Promise((resolve, reject) => {
			const {stdout} = this.db
			if (!stdout) throw new Error('DB has no stdout?')
			const rl = readline.createInterface(stdout)
			rl
				.on('line', line => {
					if (line.startsWith('Listening')) resolve()
				})
				.on('error', reject)
		})
		this.closed = new Promise(resolve => this.db.on('close', resolve))
	}

	private get port() {
		return DEFAULT_PORT + this.index
	}
	private get dataDir() {
		return `test${this.index}-data`
	}
	async sendCommand<T extends object>(command: Command, responseType: Type<T>): Promise<T> {
		await this.ready
		const client: net.Socket = net.connect(this.port)
			.on('connect', () => client.end(commandType.encode(command).finish()))
		const data = await new Promise<Uint8Array>((resolve, reject) => {
			const chunks: Buffer[] = []
			client
				.on('data', chunk => chunks.push(chunk))
				.on('end', () => resolve(concat(chunks)))
				.on('error', reject)
		})
		return responseType.toObject(
			responseType.decode(data),
			{defaults: true, longs: Number}
		)
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