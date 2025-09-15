// src/resp.ts
const CR = 0x0d; // '\r'
const LF = 0x0a; // '\n'

export class RESPDecoder {
  private buf = Buffer.alloc(0);

  // Push new data, return all complete command frames parsed as string arrays.
  push(chunk: Buffer): Array<string | null>[] {
    this.buf = Buffer.concat([this.buf, chunk] as any);
    const frames: Array<string | null>[] = [];

    let offset = 0;
    while (true) {
      const parsed = this.parseArrayOfBulkStrings(this.buf, offset);
      if (!parsed) break;
      frames.push(parsed.value);
      offset = parsed.next;
    }
    // keep any leftover (partial frame) for next data event
    this.buf = this.buf.slice(offset);

    return frames;
  }

  // Parse: *<n>\r\n ($<len>\r\n<data>\r\n) x n
  private parseArrayOfBulkStrings(
    buf: Buffer,
    i: number
  ): { value: (string | null)[]; next: number } | null {
    if (i >= buf.length || buf[i] !== 0x2a /* '*' */) return null;
    i++;

    const arrLen = this.readInteger(buf, i);
    if (!arrLen) return null;
    let { value: n, next } = arrLen;
    i = next;

    const out: (string | null)[] = [];
    for (let k = 0; k < n; k++) {
      if (i >= buf.length) return null;
      if (buf[i] !== 0x24 /* '$' */) return null;
      i++;

      const bulkLenParsed = this.readInteger(buf, i);
      if (!bulkLenParsed) return null;
      const bulkLen = bulkLenParsed.value;
      i = bulkLenParsed.next;

      if (bulkLen === -1) {
        // Null bulk string: no data bytes after the length line
        out.push(null);
        continue;
      }

      if (i + bulkLen + 2 > buf.length) return null; // wait for full payload + CRLF
      const str = buf.subarray(i, i + bulkLen).toString('utf8');
      i += bulkLen;

      if (buf[i] !== CR || buf[i + 1] !== LF) return null;
      i += 2;

      out.push(str);
    }
    return { value: out, next: i };
  }

  // Parse <integer>\r\n (supports optional leading '-')
  private readInteger(
    buf: Buffer,
    i: number
  ): { value: number; next: number } | null {
    if (i >= buf.length) return null;

    let sign = 1;
    if (buf[i] === 0x2d /* '-' */) {
      sign = -1;
      i++;
    }

    const start = i;
    while (i < buf.length && buf[i] !== CR) {
      const c = buf[i];
      if (c < 0x30 || c > 0x39) return null; // not a digit
      i++;
    }
    if (i >= buf.length) return null;
    const num = parseInt(buf.slice(start, i).toString('ascii'), 10) * sign;

    if (buf[i] !== CR || buf[i + 1] !== LF) return null;
    return { value: num, next: i + 2 };
  }
}
