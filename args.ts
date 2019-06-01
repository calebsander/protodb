import yargs = require('yargs')
import {DEFAULT_DATA_DIR, DEFAULT_PORT} from './constants'

export const {dataDir, port} = yargs.options({
	dataDir: {
		alias: 'd',
		default: DEFAULT_DATA_DIR,
		describe: 'Directory to store database files',
		string: true
	},
	port: {
		alias: 'p',
		default: DEFAULT_PORT,
		describe: 'Port for protoDB to listen on',
		number: true
	}
}).argv