import * as fs from 'fs'
import {promisify} from 'util'

const close = promisify(fs.close),
      open = promisify(fs.open),
      read = promisify(fs.read),
      stat = promisify(fs.stat),
      truncate = promisify(fs.ftruncate),
      unlink = promisify(fs.unlink),
      write = promisify(fs.write),
      writeFile = promisify(fs.writeFile)

const LOG_PAGE_SIZE = 12
export const PAGE_SIZE = 1 << LOG_PAGE_SIZE // 4096 bytes
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
			const {bytesRead} = await read(fd, data, 0, PAGE_SIZE, page << LOG_PAGE_SIZE)
			if (!bytesRead) throw new Error(`Page ${page} of file ${file} does not exist`)
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
			await write(fd, new Uint8Array(contents), 0, PAGE_SIZE, page << LOG_PAGE_SIZE)
		}
		pages.delete(page)
	}
}

export const getPageNo = (byte: number) => byte >> LOG_PAGE_SIZE
export const getPageOffset = (byte: number) => byte & (PAGE_SIZE - 1)

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

export function createFile(file: string): Promise<void> {
	return writeFile(file, EMPTY, {flag: 'wx'})
}
export function removeFile(file: string): Promise<void> {
	return unlink(file)
}
export async function getPageCount(file: string): Promise<number> {
	const {size} = await stat(file)
	if (getPageOffset(size)) throw new Error(`File ${file} contains a partial page`)
	return getPageNo(size)
}
export async function getFile(file: string): Promise<ArrayBuffer> {
	const pages = await getPageCount(file)
	const result = new Uint8Array(pages << LOG_PAGE_SIZE)
	const pagePromises: Promise<void>[] = []
	for (let offset = 0, pageNo = 0; pageNo < pages; offset += PAGE_SIZE, pageNo++) {
		pagePromises.push(new FilePage(file, pageNo).use(async page =>
			result.set(new Uint8Array(page), offset)
		))
	}
	await Promise.all(pagePromises)
	return result.buffer
}
export async function setFile(file: string, contents: ArrayBuffer): Promise<void> {
	try {
		const {fd} = await getFileCache(file)
		await truncate(fd, 0)
	}
	catch {
		await createFile(file)
	}
	const pagePromises: Promise<void>[] = []
	const {byteLength} = contents
	for (let offset = 0, pageNo = 0; offset < byteLength; offset += PAGE_SIZE, pageNo++) {
		pagePromises.push(new FilePage(file, pageNo).create(async page =>
			new Uint8Array(page).set(
				new Uint8Array(contents, offset, Math.min(PAGE_SIZE, byteLength - offset))
			)
		))
	}
	await Promise.all(pagePromises)
}