export const LOG_PAGE_SIZE: number
export const PAGE_SIZE: number

export type Callback = (err: Error | null, page: ArrayBuffer) => void
export function mmap(fd: number, offset: number, callback: Callback): void