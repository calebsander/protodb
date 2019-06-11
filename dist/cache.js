"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = require("fs");
const util_1 = require("util");
const mmap_wrapper_1 = require("./mmap-wrapper");
exports.PAGE_SIZE = mmap_wrapper_1.PAGE_SIZE;
const mmapPromise = util_1.promisify(mmap_wrapper_1.mmap);
const cache = {};
function getFileCache(file, create = false) {
    const fileCache = cache[file];
    if (fileCache)
        return fileCache;
    return cache[file] = (async () => {
        try {
            const fd = await fs_1.promises.open(file, create ? 'a+' : 'r+');
            return { pages: new Map, fd };
        }
        catch (e) {
            delete cache[file]; // if open failed, remove this file from the cache
            throw e;
        }
    })();
}
async function loadPage(file, page) {
    const { pages, fd } = await getFileCache(file);
    let pagePromise = pages.get(page);
    if (!pagePromise) {
        pages.set(page, pagePromise = mmapPromise(fd.fd, page * mmap_wrapper_1.PAGE_SIZE));
    }
    return pagePromise;
}
exports.getPageNo = (byte) => (byte / mmap_wrapper_1.PAGE_SIZE) | 0;
exports.getPageOffset = (byte) => byte & (mmap_wrapper_1.PAGE_SIZE - 1);
const pagesToFit = (bytes) => exports.getPageNo(bytes + mmap_wrapper_1.PAGE_SIZE - 1);
class FilePage {
    constructor(file, page) {
        this.file = file;
        this.page = page;
    }
    async use(consumer) {
        // We wrap the call to the consumer in case pinning is added later,
        // in which case we would need to insert pin() and unpin() calls here
        return consumer(await loadPage(this.file, this.page));
    }
}
exports.FilePage = FilePage;
exports.createFile = (file) => fs_1.promises.writeFile(file, '', { flag: 'wx' });
async function setPageCount(file, pages) {
    const { fd } = await getFileCache(file);
    await fd.truncate(pages * mmap_wrapper_1.PAGE_SIZE);
}
exports.setPageCount = setPageCount;
async function removeFile(file) {
    const promises = [fs_1.promises.unlink(file)];
    const fileCache = cache[file];
    if (fileCache) {
        delete cache[file];
        const { fd } = await fileCache;
        promises.push(fd.close());
    }
    await Promise.all(promises);
}
exports.removeFile = removeFile;
async function getPageCount(file) {
    const { fd } = await getFileCache(file);
    const { size } = await fd.stat();
    // istanbul ignore if
    if (exports.getPageOffset(size))
        throw new Error(`File ${file} contains a partial page`);
    return exports.getPageNo(size);
}
exports.getPageCount = getPageCount;
async function getFile(file, start = 0, length) {
    if (length === undefined) {
        const pageCount = await getPageCount(file);
        length = pageCount * mmap_wrapper_1.PAGE_SIZE - start;
    }
    const result = new Uint8Array(length);
    const pagePromises = [];
    for (let offset = 0, nextOffset; offset < length; offset = nextOffset) {
        const fileOffset = start + offset;
        const pageOffset = exports.getPageOffset(fileOffset);
        pagePromises.push(new FilePage(file, exports.getPageNo(fileOffset)).use(async (page) => result.set(new Uint8Array(page, pageOffset).subarray(0, length - offset), offset)));
        nextOffset = offset + mmap_wrapper_1.PAGE_SIZE - pageOffset;
    }
    await Promise.all(pagePromises);
    return result;
}
exports.getFile = getFile;
async function setFileSegment(file, contents, start, length) {
    const pagePromises = [];
    for (let offset = 0, nextOffset; offset < length; offset = nextOffset) {
        const fileOffset = start + offset;
        const pageOffset = exports.getPageOffset(fileOffset);
        nextOffset = offset + mmap_wrapper_1.PAGE_SIZE - pageOffset;
        pagePromises.push(new FilePage(file, exports.getPageNo(fileOffset)).use(async (page) => new Uint8Array(page, pageOffset).set(contents.subarray(offset, nextOffset))));
    }
    await Promise.all(pagePromises);
}
exports.setFileSegment = setFileSegment;
async function setFile(file, contents) {
    await getFileCache(file, true); // create file if it doesn't exist
    const { length } = contents;
    await setPageCount(file, pagesToFit(length));
    await setFileSegment(file, contents, 0, length);
}
exports.setFile = setFile;
async function copyWithinFile(file, source, length, target) {
    const currentPages = await getPageCount(file);
    const newLength = target + length;
    if (newLength > currentPages * mmap_wrapper_1.PAGE_SIZE) {
        await setPageCount(file, pagesToFit(newLength));
    }
    const pagePromises = [];
    for (let offset = 0, nextOffset; offset < length; offset = nextOffset) {
        // Copy to one target page at a time
        const targetOffset = target + offset;
        const pageOffset = exports.getPageOffset(targetOffset);
        const copyLength = Math.min(mmap_wrapper_1.PAGE_SIZE - pageOffset, length - offset);
        // Writes a sequence of buffers to targetOffset
        const writeBuffers = (buffers) => new FilePage(file, exports.getPageNo(targetOffset)).use(async (page) => {
            const pageArray = new Uint8Array(page);
            let offset = pageOffset;
            for (const buffer of buffers) {
                pageArray.set(buffer, offset);
                offset += buffer.length;
            }
        });
        // Obtain the source data and write it to the target location
        const sourceOffset = source + offset;
        const sourcePage = exports.getPageNo(sourceOffset);
        pagePromises.push(new FilePage(file, sourcePage).use(async (page) => {
            const pageOffset = exports.getPageOffset(sourceOffset);
            const buffer = new Uint8Array(page, pageOffset).subarray(0, copyLength);
            const remainingLength = copyLength - buffer.length;
            return remainingLength
                // Need data on part of the following page
                ? new FilePage(file, sourcePage + 1).use(async (page) => writeBuffers([buffer, new Uint8Array(page, 0, remainingLength)]))
                : writeBuffers([buffer]);
        }));
        nextOffset = offset + copyLength;
    }
    await Promise.all(pagePromises);
}
exports.copyWithinFile = copyWithinFile;
async function shutdown() {
    const closePromises = [];
    for (const file in cache) {
        const { pages, fd } = await cache[file];
        pages.clear(); // allow mmap()ed buffers to be garbage-collected and unmapped
        closePromises.push(fd.close());
    }
    await Promise.all(closePromises);
}
exports.shutdown = shutdown;
