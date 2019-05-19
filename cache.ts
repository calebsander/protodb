import fs from 'fs'
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
const pagesToFit = (bytes: number) => getPageNo(bytes + PAGE_SIZE - 1)

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
export async function getFile(file: string, start = 0, length?: number): Promise<Uint8Array> {
	if (length === undefined) {
		const pageCount = await getPageCount(file)
		length = (pageCount << LOG_PAGE_SIZE) - start
	}
	const result = new Uint8Array(length)
	const pagePromises: Promise<void>[] = []
	for (let offset = 0, nextOffset: number; offset < length; offset = nextOffset) {
		const fileOffset = start + offset
		const pageOffset = getPageOffset(fileOffset)
		pagePromises.push(new FilePage(file, getPageNo(fileOffset)).use(async page =>
			result.set(
				new Uint8Array(page, pageOffset).subarray(0, length! - offset),
				offset
			)
		))
		nextOffset = offset + PAGE_SIZE - pageOffset
	}
	await Promise.all(pagePromises)
	return result
}
export async function setFileSegment(
	file: string, contents: Uint8Array, start: number, length: number
): Promise<void> {
	const pagePromises: Promise<void>[] = []
	for (let offset = 0, nextOffset: number; offset < length; offset = nextOffset) {
		const fileOffset = start + offset
		const pageOffset = getPageOffset(fileOffset)
		nextOffset = offset + PAGE_SIZE - pageOffset
		pagePromises.push(new FilePage(file, getPageNo(fileOffset)).use(async page =>
			new Uint8Array(page, pageOffset).set(contents.subarray(offset, nextOffset))
		))
	}
	await Promise.all(pagePromises)
}
export async function setFile(file: string, contents: Uint8Array): Promise<void> {
	await getFileCache(file, true)
	await setPageCount(file, pagesToFit(contents.length))
	await setFileSegment(file, contents, 0, contents.length)
}
export async function copyWithinFile(
	file: string, source: number, length: number, target: number
): Promise<void> {
	const currentLength = await getPageCount(file) << LOG_PAGE_SIZE
	const newLength = target + length
	if (newLength > currentLength) {
		await setPageCount(file, pagesToFit(newLength))
	}
	const pagePromises: Promise<void>[] = []
	for (let offset = 0, nextOffset: number; offset < length; offset = nextOffset) {
		const targetOffset = target + offset
		const pageOffset = getPageOffset(targetOffset)
		const copyLength = Math.min(PAGE_SIZE - pageOffset, length - offset)
		const writeBuffers = (buffers: Uint8Array[]) =>
			new FilePage(file, getPageNo(targetOffset)).use(async page => {
				const pageArray = new Uint8Array(page)
				let offset = pageOffset
				for (const buffer of buffers) {
					pageArray.set(buffer, offset)
					offset += buffer.length
				}
			})
		const sourceOffset = source + offset
		const sourcePage = getPageNo(sourceOffset)
		pagePromises.push(new FilePage(file, sourcePage).use(async page => {
			const pageOffset = getPageOffset(sourceOffset)
			const buffer = new Uint8Array(page, pageOffset).subarray(0, copyLength)
			const remainingLength = copyLength - buffer.length
			return remainingLength
				? new FilePage(file, sourcePage + 1).use(async page =>
						writeBuffers([buffer, new Uint8Array(page, 0, remainingLength)])
					)
				: writeBuffers([buffer])
		}))
		nextOffset = offset + copyLength
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