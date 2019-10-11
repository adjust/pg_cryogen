#include "storage.h"
#include "compression.h"

#include "utils/guc.h"

#include "lz4.h"
#include "zstd.h"


static const struct config_enum_entry compression_method_options[] = {
	{"lz4", COMP_LZ4, false},
	{"zstd", COMP_ZSTD, false},
	{NULL, 0, false}
};

int compression_method_guc = COMP_LZ4;
int lz4_acceleration_guc = 0;
int zstd_compression_level_guc = 1;

void
cryo_define_compression_gucs(void)
{
    /* GUCs */
    DefineCustomEnumVariable("pg_cryogen.compression_method",
                             "Possible values are lz4 and zstd.",
                             NULL,
                             &compression_method_guc,
                             COMP_ZSTD,
                             compression_method_options,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomIntVariable("pg_cryogen.lz4_acceleration",
							"Sets lz4 acceleration.",
							NULL,
							&lz4_acceleration_guc,
							0,
							0, 9,
                            PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_cryogen.zstd_compression_level",
							"Sets zstd compression level.",
							NULL,
							&zstd_compression_level_guc,
							1,
							-5, 22,
                            PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
}

static char *
lz4_compress(const char *data, Size *compressed_size)
{
    Size    estimate;
    char   *compressed;

    estimate = LZ4_compressBound(CRYO_BLCKSZ);
    compressed = palloc(estimate);

    *compressed_size = LZ4_compress_fast(data, compressed,
                                         CRYO_BLCKSZ, estimate,
                                         lz4_acceleration_guc);
    if (*compressed_size == 0)
        elog(ERROR, "pg_cryogen: compression failed");

    return compressed;
}

static bool
lz4_decompress(const char *compressed, Size compressed_size, char *out)
{
    int bytes;

    bytes = LZ4_decompress_safe(compressed, out, compressed_size, CRYO_BLCKSZ);
    if (bytes < 0)
        return false;

    Assert(CRYO_BLCKSZ == bytes);

    return true;
}

static char *
zstd_compress(const char *data, Size *compressed_size)
{
    Size    estimate;
    char   *compressed;

    estimate = ZSTD_compressBound(CRYO_BLCKSZ);
    compressed = palloc(estimate);

    *compressed_size = ZSTD_compress(compressed, estimate,
                                     data, CRYO_BLCKSZ,
                                     zstd_compression_level_guc);
    if (*compressed_size == 0)
        elog(ERROR, "pg_cryogen: compression failed");

    return compressed;
}

static bool
zstd_decompress(const char *compressed, Size compressed_size, char *out)
{
    int bytes;

    bytes = ZSTD_decompress(out, CRYO_BLCKSZ, compressed, compressed_size);
    if (bytes < 0)
        return false;

    Assert(CRYO_BLCKSZ == bytes);

    return true;
}

char *
cryo_compress(CompressionMethod method,
              const char *data,
              Size *compressed_size)
{
    switch (method)
    {
        case COMP_LZ4:
            return lz4_compress(data, compressed_size);
        case COMP_ZSTD:
            return zstd_compress(data, compressed_size);
        default:
            elog(ERROR, "pg_cryogen: unknown compression method");
    }
}

/*
 * Decompress and store result in `out`
 */
bool
cryo_decompress(CompressionMethod method,
                const char *compressed,
                Size compressed_size,
                char *out)
{
    switch (method)
    {
        case COMP_LZ4:
            return lz4_decompress(compressed, compressed_size, out);
        case COMP_ZSTD:
            return zstd_decompress(compressed, compressed_size, out);
        default:
            elog(ERROR, "pg_cryogen: unknown compression method");
    }
}

