import * as fs from 'fs'
import {promisify} from 'util'

const close = promisify(fs.close),
      open = promisify(fs.open),
      read = promisify(fs.read),
      write = promisify(fs.write),
      writeFile = promisify(fs.writeFile)

export const PAGE_SIZE = 1 << 12 // 4096 bytes
const EMPTY = new Uint8Array(0)

interface CacheBuffer extends ArrayBuffer {
	dirty: boolean // TODO: can this be tracked automatically?
}
interface CachedPage {
	contents: CacheBuffer
	pinCount: number
}
interface FilePageCache {
	pages: Map<number, CachedPage>
	fd: number
}
interface PageCache {
	[file: string]: FilePageCache | undefined
}

const cache: PageCache = {}

async function getFileCache(file: string): Promise<FilePageCache> {
	let fileCache = cache[file]
	if (fileCache) return fileCache

	const fd = await open(file, 'r+')
	fileCache = cache[file] // file may have been loaded by another request
	if (fileCache) {
		await close(fd)
		return fileCache
	}

	return cache[file] = {pages: new Map, fd}
}
async function loadPage(file: string, page: number, create?: true): Promise<CacheBuffer> {
	const {pages, fd} = await getFileCache(file)
	let cachedPage = pages.get(page)
	let contents: CacheBuffer | undefined
	if (!cachedPage) {
		const data = new Uint8Array(PAGE_SIZE)
		if (!create) {
			await read(fd, data, 0, PAGE_SIZE, page * PAGE_SIZE)
			cachedPage = pages.get(page) // page may have been loaded by another request
		}
		if (!cachedPage) {
			contents = data.buffer as CacheBuffer
			contents.dirty = !!create
			pages.set(page, cachedPage = {contents, pinCount: 0})
		}
	}
	cachedPage.pinCount++
	if (!contents) ({contents} = cachedPage)
	return contents
}

async function releasePage(file: string, page: number): Promise<void> {
	const fileCache = cache[file]
	if (!fileCache) throw new Error(`File ${file} is not in cache`)
	const {pages, fd} = fileCache
	const cachedPage = pages.get(page)
	if (!cachedPage) {
		throw new Error(`Page ${page} of file ${file} is not in cache`)
	}

	// Page can be evicted if nothing is currently using it
	if (--cachedPage.pinCount === 0) {
		// TODO: maintain a cache instead of immediately flushing pages
		const {contents} = cachedPage
		if (contents.dirty) {
			await write(fd, new Uint8Array(contents), 0, PAGE_SIZE, page * PAGE_SIZE)
		}
		pages.delete(page)
	}
}

type PageConsumer<T> = (page: CacheBuffer) => Promise<T>

export class FilePage {
	constructor(readonly file: string, readonly page: number) {}

	async use<T>(consumer: PageConsumer<T>, create?: true): Promise<T> {
		const contents = await loadPage(this.file, this.page, create)
		const result = await consumer(contents)
		await releasePage(this.file, this.page)
		return result
	}
	async create<T>(consumer: PageConsumer<T>): Promise<T> {
		return this.use(consumer, true)
	}
}

export async function createFile(file: string): Promise<void> {
	await writeFile(file, EMPTY, {flag: 'wx'})
}