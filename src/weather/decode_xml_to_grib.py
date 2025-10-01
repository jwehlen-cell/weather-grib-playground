#!/usr/bin/env python3
import argparse, base64, hashlib, os, re, sys

# Streaming XML reader without building the whole tree.
# Relies on simple regex to parse our own <chunk ...>...</chunk> format safely.

CHUNK_OPEN_RE = re.compile(
    rb'<chunk\s+[^>]*i="(?P<i>\d+)"\s+off="(?P<off>\d+)"\s+len="(?P<len>\d+)"\s+sha256="(?P<sha>[0-9a-f]{64})"\s*>',
    re.I
)
CHUNK_CLOSE = b'</chunk>'
HEADER_RE = re.compile(rb'<gribpackage[^>]*sha256="(?P<sha>[0-9a-f]{64})"[^>]*>', re.I)

def stream_chunks(xml_path):
    with open(xml_path, 'rb') as f:
        buf = b''
        file_sha_hex = None
        # find header sha256
        # read in blocks
        while True:
            blk = f.read(1024*1024)
            if not blk:
                break
            buf += blk
            if file_sha_hex is None:
                m = HEADER_RE.search(buf)
                if m:
                    file_sha_hex = m.group('sha').decode('ascii')
            # extract chunks iteratively
            while True:
                m = CHUNK_OPEN_RE.search(buf)
                if not m:
                    # keep tail to avoid splitting tags
                    buf = buf[-4096:]
                    break
                start = m.start()
                # find close tag from m.end()
                close_pos = buf.find(CHUNK_CLOSE, m.end())
                if close_pos == -1:
                    # need more data
                    # keep from start for next round
                    buf = buf[start:]
                    break
                # Extract chunk content
                content_start = m.end()
                content = buf[content_start:close_pos]
                # advance buffer
                buf = buf[close_pos + len(CHUNK_CLOSE):]

                # yield chunk
                ci = int(m.group('i'))
                off = int(m.group('off'))
                blen = int(m.group('len'))
                csha = m.group('sha').decode('ascii')
                yield (ci, off, blen, csha, content), file_sha_hex

def main():
    p = argparse.ArgumentParser(description="Lossless, fast XML→GRIB reassembler (chunked).")
    p.add_argument("xml", nargs='+', help="Input XML file(s). If multiple, pass in any order; chunks are placed by 'off'.")
    p.add_argument("-o", "--out", required=True, help="Output GRIB/GRIB2 file")
    args = p.parse_args()

    # prepare output (sparse write ok)
    out_sha = hashlib.sha256()
    # We'll stage writes into a temp file then rename atomically
    tmp_out = args.out + ".part"
    if os.path.exists(tmp_out):
        os.remove(tmp_out)

    # index chunks by offset across all xml files (in case of split)
    index = []
    for x in args.xml:
        for (ci, off, blen, csha, b64), file_sha in stream_chunks(x):
            raw = base64.b64decode(b64)
            # verify per-chunk len/sha
            if len(raw) != blen:
                raise RuntimeError(f"{x}: chunk {ci} length mismatch {len(raw)} != {blen}")
            if hashlib.sha256(raw).hexdigest() != csha:
                raise RuntimeError(f"{x}: chunk {ci} sha256 mismatch")
            index.append((off, raw))

    # write in offset order
    index.sort(key=lambda t: t[0])
    with open(tmp_out, 'wb') as out:
        for off, raw in index:
            out.seek(off)
            out.write(raw)
            out_sha.update(raw)

    # rename final
    os.replace(tmp_out, args.out)
    print("Wrote:", args.out)
    print("sha256:", out_sha.hexdigest())

if __name__ == "__main__":
    sys.exit(main())
