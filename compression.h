#include "postgres.h"


char *cryo_compress(const char *data, Size *compressed_size);
bool cryo_decompress(const char *compressed, Size compressed_size, char *out);

