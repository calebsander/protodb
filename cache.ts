import * as fs from 'fs'
import {promisify} from 'util'
import {LOG_PAGE_SIZE, PAGE_SIZE, mmap} from './mmap-wrapper'

export {PAGE_SIZE}

const close = promisify(fs.close),
      open = promisify(fs.open),
      stat = promisify(fs.fstat),
      truncate = promisify(fs.ftruncate),
      unlink = promisify(fs.unlink),
      writeFile = promisify(fs.writeFile),
      mmapPromise = promisify(mmap)

interface FilePageCache {
	pages: Map<number, Promise<ArrayBuffer>>
	fd: number
}
interface PageCache {
	[file: string]: Promise<FilePageCache> | undefined
}

const cache: PageCache = {}

function getFileCache(file: string, create = false): Promise<FilePageCache> {
	const fileCache = cache[file]
	if (fileCache) return fileCache

	return cache[file] = (async () => {
		try {
			const fd = await open(file, create ? 'a+' : 'r+')
			return {pages: new Map, fd}
		}
		catch (e) {
			delete cache[file] // if open failed, remove this file from the cache
			throw e
		}
	})()
}
async function loadPage(file: string, page: number): Promise<ArrayBuffer> {
	const {pages, fd} = await getFileCache(file)
	let pagePromise = pages.get(page)
	if (!pagePromise) {
		pages.set(page, pagePromise = mmapPromise(fd, page << LOG_PAGE_SIZE))
	}
	return pagePromise
}

export const getPageNo = (byte: number) => byte >> LOG_PAGE_SIZE
export const getPageOffset = (byte: number) => byte & (PAGE_SIZE - 1)

type PageConsumer<T> = (page: ArrayBuffer) => Promise<T>

export class FilePage {
	constructor(readonly file: string, readonly page: number) {}

	async use<T>(consumer: PageConsumer<T>): Promise<T> {
		return consumer(await loadPage(this.file, this.page))
	}
}

export const createFile = (file: string): Promise<void> =>
	writeFile(file, '', {flag: 'wx'})
export async function setPageCount(file: string, pages: number): Promise<void> {
	const {fd} = await getFileCache(file)
	return truncate(fd, pages << LOG_PAGE_SIZE)
}
export async function removeFile(file: string): Promise<void> {
	const promises = [unlink(file)]
	const fileCache = cache[file]
	if (fileCache) {
		delete cache[file]
		const {fd} = await fileCache
		promises.push(close(fd))
	}
	await Promise.all(promises)
}
export async function getPageCount(file: string): Promise<number> {
	const {fd} = await getFileCache(file)
	const {size} = await stat(fd)
	if (getPageOffset(size)) throw new Error(`File ${file} contains a partial page`)
	return getPageNo(size)
}
// TODO: could getFile() and setFile() use Uint8Array instead for protobufjs?
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
	const {fd} = await getFileCache(file, true)
	const newPages = getPageNo(contents.byteLength + PAGE_SIZE - 1)
	await truncate(fd, newPages << LOG_PAGE_SIZE)
	const pagePromises: Promise<void>[] = []
	const {byteLength} = contents
	for (let offset = 0, pageNo = 0; offset < byteLength; offset += PAGE_SIZE, pageNo++) {
		pagePromises.push(new FilePage(file, pageNo).use(async page =>
			new Uint8Array(page).set(
				new Uint8Array(contents, offset, Math.min(PAGE_SIZE, byteLength - offset))
			)
		))
	}
	await Promise.all(pagePromises)
}

export async function shutdown(): Promise<void> {
	const closePromises: Promise<void>[] = []
	for (const file in cache) {
		const {pages, fd} = await cache[file]!
		pages.clear()
		closePromises.push(close(fd))
	}
	await Promise.all(closePromises)
}