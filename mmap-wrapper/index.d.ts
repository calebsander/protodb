// The number of bytes in a page. 4096 on x86.
export const PAGE_SIZE: number

export type Callback = (err: Error | null, page: ArrayBuffer) => void
export function mmap(fd: number, offset: number, callback: Callback): void