#include "storage.h"
#include "compression.h"

#include "lz4.h"


char *
cryo_compress(const char *data, Size *compressed_size)
{
    Size    estimate;
    char   *compressed;

    estimate = LZ4_compressBound(CRYO_BLCKSZ);
    compressed = palloc(estimate);

    *compressed_size = LZ4_compress_fast(data, compressed,
                                         CRYO_BLCKSZ, estimate, 0);
    if (*compressed_size == 0)
        elog(ERROR, "pg_cryogen: compression failed");

    return compressed;
}

/*
 * Decompress and store result in `out`
 */
bool
cryo_decompress(const char *compressed, Size compressed_size, char *out)
{
    int bytes;

    bytes = LZ4_decompress_safe(compressed, out, compressed_size, CRYO_BLCKSZ);
    if (bytes < 0)
        return false;

    Assert(CRYO_BLCKSZ == bytes);

    return true;
}

