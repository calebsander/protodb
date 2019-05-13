import * as path from 'path'

export const PORT = 9000

// __dirname is the "dist" folder
export const DATA_DIR = path.join(__dirname, '..', 'data')

export const ITER_BYTE_LENGTH = 16