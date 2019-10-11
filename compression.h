#ifndef __COMPRESSION_H__
#define __COMPRESSION_H__

#include "postgres.h"


typedef enum
{
    COMP_LZ4 = 0,
    COMP_ZSTD
} CompressionMethod;

int compression_method_guc;
int lz4_acceleration_guc;
int zstd_compression_level_guc;

char *cryo_compress(CompressionMethod method,
                    const char *data,
                    Size *compressed_size);
bool cryo_decompress(CompressionMethod method,
                     const char *compressed,
                     Size compressed_size,
                     char *out);
void cryo_define_compression_gucs(void);

#endif /* __COMPRESSION_H__ */
