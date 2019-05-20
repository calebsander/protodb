import {ChildProcess, exec, spawn} from 'child_process'
import net from 'net'
import readline from 'readline'
import {promisify} from 'util'
import {DEFAULT_PORT} from '../constants'
import {Type} from '../pb/common'
import {Command, commandType} from '../pb/request'
import {concat} from '../util'

const DB_PATH = 'dist/index.js'
const DATA_DIR = 'test-data'

const execPromise = promisify(exec)

export class TestContext {
	private readonly db: ChildProcess
	private readonly ready: Promise<void>
	private readonly closed: Promise<void>

	constructor() {
		this.db = spawn(DB_PATH, ['-p', `${DEFAULT_PORT}`, '-d', DATA_DIR])
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

	async sendCommand<T extends object>(command: Command, responseType: Type<T>): Promise<T> {
		await this.ready
		const client: net.Socket = net.createConnection(DEFAULT_PORT)
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
		await execPromise(`rm -rf ${DATA_DIR}`)
	}
}