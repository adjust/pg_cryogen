#ifndef __COMPRESSION_H__
#define __COMPRESSION_H__

#include "postgres.h"


typedef enum
{
    COMP_LZ4 = 0,
    COMP_ZSTD
} CompressionMethod;

char *cryo_compress(CompressionMethod method,
                    const char *data,
                    Size *compressed_size);
bool cryo_decompress(CompressionMethod method,
                     const char *compressed,
                     Size compressed_size,
                     char *out);

#endif /* __COMPRESSION_H__ */
