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
	pages: Map<number, Promise<CachedPage>>
	fd: number
}
interface PageCache {
	[file: string]: Promise<FilePageCache> | undefined
}

const cache: PageCache = {}

function getFileCache(file: string): Promise<FilePageCache> {
	const fileCache = cache[file]
	if (fileCache) return fileCache

	return cache[file] = (async () => {
		try {
			const fd = await open(file, 'r+')
			return {pages: new Map, fd}
		}
		catch (e) {
			delete cache[file] // if open failed, remove this file from the cache
			throw e
		}
	})()
}
async function loadPage(file: string, page: number, create?: true): Promise<CacheBuffer> {
	const {pages, fd} = await getFileCache(file)
	let pagePromise = pages.get(page)
	if (!pagePromise) {
		pages.set(page, pagePromise = (async () => {
			const data = new Uint8Array(PAGE_SIZE)
			if (!create) {
				const {bytesRead} = await read(fd, data, 0, PAGE_SIZE, page << LOG_PAGE_SIZE)
				if (!bytesRead) throw new Error(`Page ${page} of file ${file} does not exist`)
			}
			const contents = data.buffer as CacheBuffer
			contents.dirty = !!create
			return {contents, pinCount: 0}
		})())
	}
	const cachedPage = await pagePromise
	cachedPage.pinCount++
	return cachedPage.contents
}

async function flushPage(
	fd: number, page: number, contents: CacheBuffer
): Promise<void> {
	if (contents.dirty) {
		await write(fd, new Uint8Array(contents), 0, PAGE_SIZE, page << LOG_PAGE_SIZE)
	}
}

async function releasePage(file: string, page: number): Promise<void> {
	const fileCache = cache[file]
	if (!fileCache) throw new Error(`File ${file} is not in cache`)
	const {pages, fd} = await fileCache
	const pagePromise = pages.get(page)
	if (!pagePromise) {
		throw new Error(`Page ${page} of file ${file} is not in cache`)
	}

	// Page can be evicted if nothing is currently using it
	const cachedPage = await pagePromise
	if (!--cachedPage.pinCount) {
		// TODO: maintain a cache instead of immediately flushing pages
		pages.delete(page)
		await flushPage(fd, page, cachedPage.contents)
	}
}

export const getPageNo = (byte: number) => byte >> LOG_PAGE_SIZE
export const getPageOffset = (byte: number) => byte & (PAGE_SIZE - 1)

type PageConsumer<T> = (page: CacheBuffer) => Promise<T>

export class FilePage {
	constructor(readonly file: string, readonly page: number) {}

	async use<T>(consumer: PageConsumer<T>, create?: true): Promise<T> {
		const contents = await loadPage(this.file, this.page, create)
		try {
			return await consumer(contents)
		}
		finally {
			await releasePage(this.file, this.page)
		}
	}
	async create<T>(consumer: PageConsumer<T>): Promise<T> {
		return this.use(consumer, true)
	}
}

export function createFile(file: string): Promise<void> {
	return writeFile(file, EMPTY, {flag: 'wx'})
}
export async function removeFile(file: string): Promise<void> {
	const pagePromise = cache[file]
	if (pagePromise) {
		delete cache[file]
		const {fd} = await pagePromise
		await close(fd)
	}
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

export async function shutdown(): Promise<void> {
	const savePromises: Promise<void>[] = []
	const fds: number[] = []
	for (const file in cache) {
		const {pages, fd} = await cache[file]!
		for (const [page, pagePromise] of pages) {
			savePromises.push((async () => {
				const {contents} = await pagePromise
				return flushPage(fd, page, contents)
			})())
		}
		fds.push(fd)
	}
	await Promise.all(savePromises)
	await Promise.all(fds.map(fd => close(fd)))
}