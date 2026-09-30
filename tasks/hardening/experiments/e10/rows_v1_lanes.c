/* rows-v1 lanes: one SHA-256 per canonical line, two 64 bit lane sums.
 *
 * E-10 prototype. Input: an Arrow string array's int32 offsets and data buffer.
 * For every line: SHA-256 of its UTF-8 bytes; lane a is the digest's first 8
 * bytes read big endian, lane b the next 8. Both are summed modulo 2**64 into
 * out[0] and out[1] (added to what is there). This is exactly what
 * ubunye.lineage.content_hash._arrow_lanes computes, without one Python call
 * per row.
 *
 * SHA-256 is computed with the x86 SHA extensions when the CPU has them
 * (checked at run time), else with a plain C implementation. No Python API is
 * used, so the library is loaded with ctypes, which releases the GIL during
 * the call: threads can hash slices at the same time.
 */
#include <stdint.h>
#include <string.h>

#if defined(_MSC_VER)
#include <intrin.h>
#include <immintrin.h>
#define EXPORT __declspec(dllexport)
#define TARGET_SHA
#else
#include <cpuid.h>
#include <immintrin.h>
#define EXPORT __attribute__((visibility("default")))
#define TARGET_SHA __attribute__((target("sha,sse4.1,ssse3")))
#endif

static const uint32_t K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

static const uint32_t H0[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                               0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

#define ROR(x, n) (((x) >> (n)) | ((x) << (32 - (n))))

static void compress_c(uint32_t st[8], const uint8_t *p, size_t blocks) {
    uint32_t w[64];
    while (blocks--) {
        int t;
        for (t = 0; t < 16; t++)
            w[t] = ((uint32_t)p[4 * t] << 24) | ((uint32_t)p[4 * t + 1] << 16) |
                   ((uint32_t)p[4 * t + 2] << 8) | (uint32_t)p[4 * t + 3];
        for (t = 16; t < 64; t++) {
            uint32_t s0 = ROR(w[t - 15], 7) ^ ROR(w[t - 15], 18) ^ (w[t - 15] >> 3);
            uint32_t s1 = ROR(w[t - 2], 17) ^ ROR(w[t - 2], 19) ^ (w[t - 2] >> 10);
            w[t] = w[t - 16] + s0 + w[t - 7] + s1;
        }
        uint32_t a = st[0], b = st[1], c = st[2], d = st[3];
        uint32_t e = st[4], f = st[5], g = st[6], h = st[7];
        for (t = 0; t < 64; t++) {
            uint32_t S1 = ROR(e, 6) ^ ROR(e, 11) ^ ROR(e, 25);
            uint32_t ch = (e & f) ^ (~e & g);
            uint32_t t1 = h + S1 + ch + K[t] + w[t];
            uint32_t S0 = ROR(a, 2) ^ ROR(a, 13) ^ ROR(a, 22);
            uint32_t mj = (a & b) ^ (a & c) ^ (b & c);
            uint32_t t2 = S0 + mj;
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        st[0] += a; st[1] += b; st[2] += c; st[3] += d;
        st[4] += e; st[5] += f; st[6] += g; st[7] += h;
        p += 64;
    }
}

TARGET_SHA static void compress_ni(uint32_t st[8], const uint8_t *p, size_t blocks) {
    const __m128i MASK = _mm_set_epi64x(0x0c0d0e0f08090a0bULL, 0x0405060700010203ULL);
    __m128i TMP = _mm_loadu_si128((const __m128i *)&st[0]);
    __m128i S1 = _mm_loadu_si128((const __m128i *)&st[4]);
    TMP = _mm_shuffle_epi32(TMP, 0xB1);      /* CDAB */
    S1 = _mm_shuffle_epi32(S1, 0x1B);        /* EFGH */
    __m128i S0 = _mm_alignr_epi8(TMP, S1, 8); /* ABEF */
    S1 = _mm_blend_epi16(S1, TMP, 0xF0);     /* CDGH */
    while (blocks--) {
        __m128i A0 = S0, C0 = S1;
        __m128i W0 = _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(p + 0)), MASK);
        __m128i W1 = _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(p + 16)), MASK);
        __m128i W2 = _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(p + 32)), MASK);
        __m128i W3 = _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(p + 48)), MASK);
        __m128i M;
#define RND4(Wg, g)                                                                  \
    M = _mm_add_epi32((Wg), _mm_loadu_si128((const __m128i *)&K[4 * (g)]));          \
    S1 = _mm_sha256rnds2_epu32(S1, S0, M);                                           \
    M = _mm_shuffle_epi32(M, 0x0E);                                                  \
    S0 = _mm_sha256rnds2_epu32(S0, S1, M);
/* next group from the four before it: Wa is g-4, Wb g-3, Wc g-2, Wd g-1 */
#define NEXT(Wa, Wb, Wc, Wd)                                                         \
    Wa = _mm_sha256msg2_epu32(                                                       \
        _mm_add_epi32(_mm_sha256msg1_epu32(Wa, Wb), _mm_alignr_epi8(Wd, Wc, 4)), Wd);
        RND4(W0, 0) RND4(W1, 1) RND4(W2, 2) RND4(W3, 3)
        NEXT(W0, W1, W2, W3) RND4(W0, 4)
        NEXT(W1, W2, W3, W0) RND4(W1, 5)
        NEXT(W2, W3, W0, W1) RND4(W2, 6)
        NEXT(W3, W0, W1, W2) RND4(W3, 7)
        NEXT(W0, W1, W2, W3) RND4(W0, 8)
        NEXT(W1, W2, W3, W0) RND4(W1, 9)
        NEXT(W2, W3, W0, W1) RND4(W2, 10)
        NEXT(W3, W0, W1, W2) RND4(W3, 11)
        NEXT(W0, W1, W2, W3) RND4(W0, 12)
        NEXT(W1, W2, W3, W0) RND4(W1, 13)
        NEXT(W2, W3, W0, W1) RND4(W2, 14)
        NEXT(W3, W0, W1, W2) RND4(W3, 15)
        S0 = _mm_add_epi32(S0, A0);
        S1 = _mm_add_epi32(S1, C0);
        p += 64;
    }
    TMP = _mm_shuffle_epi32(S0, 0x1B);       /* FEBA */
    S1 = _mm_shuffle_epi32(S1, 0xB1);        /* DCHG */
    S0 = _mm_blend_epi16(TMP, S1, 0xF0);     /* DCBA */
    S1 = _mm_alignr_epi8(S1, TMP, 8);        /* ABEF */
    _mm_storeu_si128((__m128i *)&st[0], S0);
    _mm_storeu_si128((__m128i *)&st[4], S1);
}

static int use_ni = -1;

static int cpu_has_sha(void) {
#if defined(_MSC_VER)
    int r[4];
    __cpuid(r, 0);
    if (r[0] < 7) return 0;
    __cpuidex(r, 7, 0);
    int sha = (r[1] >> 29) & 1;
    __cpuid(r, 1);
    int sse41 = (r[2] >> 19) & 1, ssse3 = (r[2] >> 9) & 1;
    return sha && sse41 && ssse3;
#else
    unsigned a, b, c, d;
    if (!__get_cpuid_count(7, 0, &a, &b, &c, &d)) return 0;
    int sha = (b >> 29) & 1;
    __get_cpuid(1, &a, &b, &c, &d);
    return sha && ((c >> 19) & 1) && ((c >> 9) & 1);
#endif
}

EXPORT int rows_v1_has_shani(void) { return cpu_has_sha(); }

/* 1: SHA extensions, 0: plain C, -1: decide by the CPU. Returns the mode now used. */
EXPORT int rows_v1_set_mode(int mode) {
    use_ni = (mode < 0) ? cpu_has_sha() : (mode && cpu_has_sha());
    return use_ni;
}

static void sha256_line(const uint8_t *p, size_t len, uint32_t st[8]) {
    uint8_t buf[128];
    size_t full = len & ~(size_t)63, rem = len - full, tot;
    uint64_t bits = (uint64_t)len * 8;
    memcpy(st, H0, sizeof(H0));
    if (full) {
        if (use_ni) compress_ni(st, p, full / 64);
        else compress_c(st, p, full / 64);
    }
    memcpy(buf, p + full, rem);
    buf[rem] = 0x80;
    tot = (rem + 9 <= 64) ? 64 : 128;
    memset(buf + rem + 1, 0, tot - rem - 1 - 8);
    for (int i = 0; i < 8; i++) buf[tot - 1 - i] = (uint8_t)(bits >> (8 * i));
    if (use_ni) compress_ni(st, buf, tot / 64);
    else compress_c(st, buf, tot / 64);
}

/* Lane sums of n lines: line i is data[off[i]:off[i+1]]. Adds into out[0], out[1]. */
EXPORT void rows_v1_lanes(const int32_t *off, int64_t n, const uint8_t *data, uint64_t *out) {
    uint32_t st[8];
    uint64_t a = 0, b = 0;
    if (use_ni < 0) use_ni = cpu_has_sha();
    for (int64_t i = 0; i < n; i++) {
        sha256_line(data + off[i], (size_t)(off[i + 1] - off[i]), st);
        a += ((uint64_t)st[0] << 32) | st[1];
        b += ((uint64_t)st[2] << 32) | st[3];
    }
    out[0] += a;
    out[1] += b;
}

/* One digest, for tests: the 32 bytes of SHA-256(p[0:len]). */
EXPORT void rows_v1_sha256(const uint8_t *p, int64_t len, uint8_t *digest) {
    uint32_t st[8];
    if (use_ni < 0) use_ni = cpu_has_sha();
    sha256_line(p, (size_t)len, st);
    for (int i = 0; i < 8; i++) {
        digest[4 * i] = (uint8_t)(st[i] >> 24);
        digest[4 * i + 1] = (uint8_t)(st[i] >> 16);
        digest[4 * i + 2] = (uint8_t)(st[i] >> 8);
        digest[4 * i + 3] = (uint8_t)st[i];
    }
}
