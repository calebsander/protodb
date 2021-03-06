import {promises as fs} from 'fs'
import {promisify} from 'util'
import {mmap, PAGE_SIZE} from './mmap-wrapper'

// Re-export PAGE_SIZE so other modules don't include mmap-wrapper directly
export {PAGE_SIZE}

const mmapPromise = promisify(mmap)

interface FilePageCache {
	pages: Map<number, Promise<ArrayBuffer>>
	fd: fs.FileHandle
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
			const fd = await fs.open(file, create ? 'a+' : 'r+')
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
		pages.set(page, pagePromise = mmapPromise(fd.fd, page * PAGE_SIZE))
	}
	return pagePromise
}

export const getPageNo = (byte: number) => (byte / PAGE_SIZE) | 0
export const getPageOffset = (byte: number) => byte & (PAGE_SIZE - 1)
const pagesToFit = (bytes: number) => getPageNo(bytes + PAGE_SIZE - 1)

type PageConsumer<T> = (page: ArrayBuffer) => Promise<T>

export class FilePage {
	constructor(readonly file: string, readonly page: number) {}

	async use<T>(consumer: PageConsumer<T>): Promise<T> {
		// We wrap the call to the consumer in case pinning is added later,
		// in which case we would need to insert pin() and unpin() calls here
		return consumer(await loadPage(this.file, this.page))
	}
}

export const createFile = (file: string): Promise<void> =>
	fs.writeFile(file, '', {flag: 'wx'})
export async function setPageCount(file: string, pages: number): Promise<void> {
	const {fd} = await getFileCache(file)
	await fd.truncate(pages * PAGE_SIZE)
}
export async function removeFile(file: string): Promise<void> {
	const promises = [fs.unlink(file)]
	const fileCache = cache[file]
	if (fileCache) {
		delete cache[file]
		const {fd} = await fileCache
		promises.push(fd.close())
	}
	await Promise.all(promises)
}
export async function getPageCount(file: string): Promise<number> {
	const {fd} = await getFileCache(file)
	const {size} = await fd.stat()
	// istanbul ignore if
	if (getPageOffset(size)) throw new Error(`File ${file} contains a partial page`)
	return getPageNo(size)
}
export async function getFile(file: string, start = 0, length?: number): Promise<Uint8Array> {
	if (length === undefined) {
		const pageCount = await getPageCount(file)
		length = pageCount * PAGE_SIZE - start
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
	await getFileCache(file, true) // create file if it doesn't exist
	const {length} = contents
	await setPageCount(file, pagesToFit(length))
	await setFileSegment(file, contents, 0, length)
}
export async function copyWithinFile(
	file: string, source: number, length: number, target: number
): Promise<void> {
	const currentPages = await getPageCount(file)
	const newLength = target + length
	if (newLength > currentPages * PAGE_SIZE) {
		await setPageCount(file, pagesToFit(newLength))
	}
	const pagePromises: Promise<void>[] = []
	for (let offset = 0, nextOffset: number; offset < length; offset = nextOffset) {
		// Copy to one target page at a time
		const targetOffset = target + offset
		const pageOffset = getPageOffset(targetOffset)
		const copyLength = Math.min(PAGE_SIZE - pageOffset, length - offset)
		// Writes a sequence of buffers to targetOffset
		const writeBuffers = (buffers: Uint8Array[]) =>
			new FilePage(file, getPageNo(targetOffset)).use(async page => {
				const pageArray = new Uint8Array(page)
				let offset = pageOffset
				for (const buffer of buffers) {
					pageArray.set(buffer, offset)
					offset += buffer.length
				}
			})
		// Obtain the source data and write it to the target location
		const sourceOffset = source + offset
		const sourcePage = getPageNo(sourceOffset)
		pagePromises.push(new FilePage(file, sourcePage).use(async page => {
			const pageOffset = getPageOffset(sourceOffset)
			const buffer = new Uint8Array(page, pageOffset).subarray(0, copyLength)
			const remainingLength = copyLength - buffer.length
			return remainingLength
				// Need data on part of the following page
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
		pages.clear() // allow mmap()ed buffers to be garbage-collected and unmapped
		closePromises.push(fd.close())
	}
	await Promise.all(closePromises)
}