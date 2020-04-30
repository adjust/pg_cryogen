#ifndef __COMPRESSION_H__
#define __COMPRESSION_H__

#include "postgres.h"


typedef enum
{
    COMP_LZ4 = 0,
    COMP_ZSTD
} CompressionMethod;

extern int compression_method_guc;
extern int lz4_acceleration_guc;
extern int zstd_compression_level_guc;

extern char *cryo_compress(CompressionMethod method,
                           const char *data,
                           Size *compressed_size);
extern bool cryo_decompress(CompressionMethod method,
                            const char *compressed,
                            Size compressed_size,
                            char *out);
extern void cryo_define_compression_gucs(void);

#endif /* __COMPRESSION_H__ */
