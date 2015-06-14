/* byte of the bitstring bit is in */
#define	__bit_byte(bit)	((bit) >> 3)

/* mask for the bit within its byte */
#define	__bit_mask(bit)	(1 << ((bit) & 0x7))

/* Bytes in a bitstring of nbits */
#define	__bitstr_size(nbits) (((nbits) + 7) >> 3)

static inline int __bit_alloc(WT_SESSION_IMPL *session, uint64_t nbits, void *retp)
{
	return __wt_calloc(session, (size_t)__bitstr_size(nbits), sizeof(uint8_t), retp);
}

static inline int __bit_test(uint8_t *bitf, uint64_t bit)
{
	return (bitf[__bit_byte(bit)] & __bit_mask(bit) ? 1 : 0);
}

static inline void __bit_set(uint8_t *bitf, uint64_t bit)
{
	bitf[__bit_byte(bit)] |= __bit_mask(bit);
}

static inline void __bit_clear(uint8_t *bitf, uint64_t bit)
{
	bitf[__bit_byte(bit)] &= ~__bit_mask(bit);
}

/*bit位范围清除*/
static inline void __bit_nclr(uint8_t *bitf, uint64_t start, uint64_t stop)
{
	uint64_t startbyte, stopbyte;

	startbyte = __bit_byte(start);
	stopbyte = __bit_byte(stop);

	if (startbyte == stopbyte)
		bitf[startbyte] &= ((0xff >> (8 - (start & 0x7))) | (0xff << ((stop & 0x7) + 1)));
	else {
		bitf[startbyte] &= 0xff >> (8 - (start & 0x7));
		while (++startbyte < stopbyte)
			bitf[startbyte] = 0;
		bitf[stopbyte] &= 0xff << ((stop & 0x7) + 1);
	}
}

/*bit范围设置*/
static inline void __bit_nset(uint8_t *bitf, uint64_t start, uint64_t stop)
{
	uint64_t startbyte, stopbyte;

	startbyte = __bit_byte(start);
	stopbyte = __bit_byte(stop);
	if (startbyte == stopbyte)
		bitf[startbyte] |= ((0xff << (start & 0x7)) & (0xff >> (7 - (stop & 0x7))));
	else {
		bitf[startbyte] |= 0xff << (start & 0x7);
		while (++startbyte < stopbyte)
			bitf[startbyte] = 0xff;
		bitf[stopbyte] |= 0xff >> (7 - (stop & 0x7));
	}
}

static inline int __bit_ffc(uint8_t *bitf, uint64_t nbits, uint64_t *retp)
{
	uint8_t lb;
	uint64_t byte, stopbyte, value;

	value = 0;		/* -Wuninitialized */

	if (nbits == 0)
		return (-1);

	for (byte = 0,stopbyte = __bit_byte(nbits - 1); byte <= stopbyte; ++byte){
		if (bitf[byte] != 0xff) {
			value = byte << 3;
			for (lb = bitf[byte]; lb & 0x01; ++value, lb >>= 1)
				;
			break;
		}
	}

	if (byte > stopbyte || value >= nbits)
		return (-1);

	*retp = value;
	return 0;
}

static inline int __bit_ffs(uint8_t *bitf, uint64_t nbits, uint64_t *retp)
{
	uint8_t lb;
	uint64_t byte, stopbyte, value;

	value = 0;
	if (nbits == 0)
		return (-1);

	for (byte = 0,
		stopbyte = __bit_byte(nbits - 1); byte <= stopbyte; ++byte){
			if (bitf[byte] != 0) {
				value = byte << 3;
				for (lb = bitf[byte]; !(lb & 0x01); ++value, lb >>= 1)
					;
				break;
			}
	}
	if (byte > stopbyte || value >= nbits)
		return (-1);

	*retp = value;
	return (0);
}

#define	__BIT_GET(len, mask)					\
	case len:									\
	if (__bit_test(bitf, bit))					\
	value |= mask;								\
	++bit							

/*获取entry开始的width个bit的值*/
static inline uint8_t __bit_getv(uint8_t *bitf, uint64_t entry, uint8_t width)
{
	uint8_t value;
	uint64_t bit;

	value = 0;
	bit = entry * width;

	/*
	 * Fast-path single bytes, do repeated tests for the rest: we could
	 * slice-and-dice instead, but the compiler is probably going to do
	 * a better job than I will.
	 */
	switch (width) {
	case 8:
		return (bitf[__bit_byte(bit)]);
	__BIT_GET(7, 0x40);
	__BIT_GET(6, 0x20);
	__BIT_GET(5, 0x10);
	__BIT_GET(4, 0x08);
	__BIT_GET(3, 0x04);
	__BIT_GET(2, 0x02);
	__BIT_GET(1, 0x01);
	}
	return (value);
}

static inline uint8_t __bit_getv_recno(WT_PAGE *page, uint64_t recno, uint8_t width)
{
	return (__bit_getv(page->pg_fix_bitf, recno - page->pg_fix_recno, width));
}

#define	__BIT_SET(len, mask)						\
	case len:										\
	if (value & (mask))								\
	__bit_set(bitf, bit);							\
	else											\
	__bit_clear(bitf, bit);							\
	++bit							

/*设置entry开始的width个bit的值*/
static inline void __bit_setv(uint8_t *bitf, uint64_t entry, uint8_t width, uint8_t value)
{
	uint64_t bit;
	bit = entry * width;

	/*
	 * Fast-path single bytes, do repeated tests for the rest: we could
	 * slice-and-dice instead, but the compiler is probably going to do
	 * a better job than I will.
	 */
	switch(width) {
	case 8:
		bitf[__bit_byte(bit)] = value;
		return;
	__BIT_SET(7, 0x40);
	__BIT_SET(6, 0x20);
	__BIT_SET(5, 0x10);
	__BIT_SET(4, 0x08);
	__BIT_SET(3, 0x04);
	__BIT_SET(2, 0x02);
	__BIT_SET(1, 0x01);
	}
}

static inline void __bit_setv_recno(WT_PAGE *page, uint64_t recno, uint8_t width, uint8_t value)
{
	__bit_setv(page->pg_fix_bitf, recno - page->pg_fix_recno, width, value);
}




