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
const FRAMES = 3
const EMPTY = new Uint8Array(0)

interface CacheBuffer extends ArrayBuffer {
	dirty: boolean // TODO: can this be tracked automatically?
}
interface Frame {
	fileCache: FilePageCache
	page: number
	contents: CacheBuffer
	pinCount: number
	accessed: boolean
}
interface FilePageCache {
	pages: Map<number, Frame | Promise<Frame>>
	fd: number
}
interface PageCache {
	[file: string]: FilePageCache | Promise<FilePageCache> | undefined
}

const frames = new Array<Frame | undefined>(FRAMES)
let clockIndex = 0

async function makeFrame(fileCache: FilePageCache, page: number): Promise<Frame> {
	while (true) {
		const frame = frames[clockIndex]
		if (!frame) break

		if (!frame.pinCount) {
			if (!frame.accessed) break

			frame.accessed = false
		}
		clockIndex = (clockIndex + 1) % FRAMES
	}
	let frame = frames[clockIndex]
	if (frame) {
		frame.pinCount = 1 // prevent another call from evicting the same frame
		const {fileCache: {pages}, page} = frame
		pages.delete(page)
		await flushPage(frame)
		frame.pinCount = 0
	}
	else {
		frame = frames[clockIndex] = {
			fileCache,
			page,
			contents: new Uint8Array(PAGE_SIZE).buffer as CacheBuffer,
			pinCount: 0,
			accessed: false
		}
	}
	frame.contents.dirty = false
	access(frame)
	return frame
}
function access(frame: Frame): void {
	frame.pinCount++
	frame.accessed = true
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
async function loadPage(file: string, page: number, create?: true): Promise<Frame> {
	const fileCache = await getFileCache(file)
	const {pages, fd} = fileCache
	let pagePromise = pages.get(page)
	if (pagePromise) {
		const cachedPage = await pagePromise
		access(cachedPage)
		return cachedPage
	}
	else {
		pages.set(page, pagePromise = (async () => {
			const frame = await makeFrame(fileCache, page)
			const {contents} = frame
			if (!create) {
				const {bytesRead} = await read(
					fd, new Uint8Array(contents), 0, PAGE_SIZE, page << LOG_PAGE_SIZE
				)
				if (!bytesRead) throw new Error(`Page ${page} of file ${file} does not exist`)
			}
			return frame
		})())
		return pagePromise
	}
}

async function flushPage({fileCache: {fd}, page, contents}: Frame): Promise<void> {
	if (contents.dirty) {
		await write(fd, new Uint8Array(contents), 0, PAGE_SIZE, page << LOG_PAGE_SIZE)
	}
}

export const getPageNo = (byte: number) => byte >> LOG_PAGE_SIZE
export const getPageOffset = (byte: number) => byte & (PAGE_SIZE - 1)

type PageConsumer<T> = (page: CacheBuffer) => Promise<T>

export class FilePage {
	constructor(readonly file: string, readonly page: number) {}

	async use<T>(consumer: PageConsumer<T>, create?: true): Promise<T> {
		const frame = await loadPage(this.file, this.page, create)
		const {contents} = frame
		if (create) contents.dirty = true
		try {
			return await consumer(contents)
		}
		finally {
			--frame.pinCount
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
	for (const frame of frames) {
		if (!frame) break

		savePromises.push(flushPage(frame))
	}
	await Promise.all(savePromises)
	const closePromises: Promise<void>[] = []
	for (const file in cache) {
		const {fd} = await cache[file]!
		closePromises.push(close(fd))
	}
	await Promise.all(closePromises)
}