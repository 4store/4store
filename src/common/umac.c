/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/* -----------------------------------------------------------------------
 * 
 * umac.c -- C Implementation UMAC Message Authentication
 *
 * Version 0.92 of draft-krovetz-umac-07.txt -- 2006 February 21
 *
 * For a full description of UMAC message authentication see the UMAC
 * world-wide-web page at http://www.cs.ucdavis.edu/~rogaway/umac
 * Please report bugs and suggestions to the UMAC webpage.
 *
 * Copyright (c) 1999-2006 Ted Krovetz
 *                                                                 
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and with or without fee, is hereby
 * granted provided that the above copyright notice appears in all copies
 * and in supporting documentation, and that the name of the copyright
 * holder not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior permission.
 *
 * Comments should be directed to Ted Krovetz (tdk@acm.org)                                        
 *                                                                   
 * ---------------------------------------------------------------------- */
 
 /* ////////////////////// IMPORTANT NOTES /////////////////////////////////
  *
  * 1) This version does not work properly on messages larger than 16MB
  *
  * 2) If you set the switch to use SSE2, then all data must be 16-byte
  *    aligned
  *
  * 3) When calling the function umac(), it is assumed that msg is in
  * a writable buffer of length divisible by 32 bytes. The message itself
  * does not have to fill the entire buffer, but bytes beyond msg may be
  * zeroed.
  *
  * 4) Two free AES implementations are supported by this implementation of
  * UMAC. Paulo Barreto's version is in the public domain and can be found
  * at http://www.esat.kuleuven.ac.be/~rijmen/rijndael/ (search for
  * "Barreto"). The only two files needed are rijndael-alg-fst.c and
  * rijndael-alg-fst.h. Brian Gladman's version is distributed with the GNU
  * Public lisence at http://fp.gladman.plus.com/AES/index.htm. It
  * includes a fast IA-32 assembly version.
  *
  * 5) With FORCE_C_ONLY flags set to 0, incorrect results are sometimes
  * produced under gcc with optimizations set -O3 or higher. Dunno why.
  *
  /////////////////////////////////////////////////////////////////////// */
 
/* ---------------------------------------------------------------------- */
/* --- User Switches ---------------------------------------------------- */
/* ---------------------------------------------------------------------- */

#ifndef UMAC_OUTPUT_LEN
#  define UMAC_OUTPUT_LEN    8    /* Alowable: 4, 8, 12, 16               */
#endif
#ifndef FORCE_C_ONLY
#  define FORCE_C_ONLY         1  /* ANSI C and 64-bit integers req'd     */
#endif
#ifndef GLADMAN_AES
#  define GLADMAN_AES          0  /* Change to 1 to use Gladman's AES     */
#endif

#define SSE2                 0

/* ---------------------------------------------------------------------- */
/* -- Global Includes --------------------------------------------------- */
/* ---------------------------------------------------------------------- */

#include "../common/umac.h"

#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#if GLADMAN_AES
#include "aes.h"
#else
#include "rijndael-alg-fst.h"
#endif

/* ---------------------------------------------------------------------- */
/* --- Primitive Data Types ---                                           */
/* ---------------------------------------------------------------------- */

/* The following assumptions may need change on your system */
typedef unsigned char      UINT8;  /* 1 byte   */
typedef unsigned short     UINT16; /* 2 byte   */
typedef unsigned int       UINT32; /* 4 byte   */
typedef unsigned long long UINT64; /* 8 bytes  */

#if __WORDSIZE == 64
typedef UINT64 UWORD;
#else
typedef UINT32 UWORD;
#endif // __WORDSIZE == 64

/* ---------------------------------------------------------------------- */
/* --- Constants ------------------------------------------------ */
/* ---------------------------------------------------------------------- */

#define UMAC_KEY_LEN           16  /* UMAC takes 16 bytes of external key */

/* GNU gcc and Microsoft Visual C++ (and copycats) on IA-32 are supported
 * with some assembly
 */
#define GCC_X86         (__GNUC__ && (__i386__ || __x86_64__))   /* GCC on IA-32       */
#define MSC_X86         (_M_IX86)                   /* Microsoft on IA-32 */

/* Message "words" are read from memory in an endian-specific manner.     */
/* For this implementation to behave correctly, __LITTLE_ENDIAN__ must    */
/* be set true if the host computer is little-endian.                     */

#ifndef __LITTLE_ENDIAN__
#if __i386__ || __alpha__ || _M_IX86 || __LITTLE_ENDIAN
#define __LITTLE_ENDIAN__ 1
#else
#define __LITTLE_ENDIAN__ 0
#endif
#endif

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Architecture Specific ------------------------------------------ */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

#if (MSC_X86)
#pragma warning(disable: 4731)  /* Turn off "ebp manipulation" warning    */
#pragma warning(disable: 4311)  /* Turn off "pointer trunc" warning       */
#if (__MWERKS__)
#define mmword xmmword   /* Metrowerks C 3.03 doesn't recognize mmword */
#endif
#endif

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Primitive Routines --------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* --- 32-bit by 32-bit to 64-bit Multiplication ------------------------ */
/* ---------------------------------------------------------------------- */

#define MUL64(a,b) ((UINT64)((UINT64)(UINT32)(a) * (UINT64)(UINT32)(b)))

/* ---------------------------------------------------------------------- */
/* --- Endian Conversion --- Forcing assembly on some platforms           */
/* ---------------------------------------------------------------------- */

/* Lots of endian reversals happen in UMAC. PowerPC and Intel Architechture
 * both support efficient endian conversion, but compilers seem unable to
 * automatically utilize the efficient assembly opcodes. The architechture-
 * specific versions utilize them.
 */
               
#if (MSC_X86 && ! FORCE_C_ONLY)

static UINT32 LOAD_UINT32_REVERSED(void *p)
{
    __asm {
        mov eax, p
        mov eax, [eax]
        bswap eax
    }
}

static void STORE_UINT32_REVERSED(void *p, UINT32 x)
{
    __asm {
        mov eax,x
        bswap eax
        mov ecx, p
        mov [ecx], eax
    }
}

#elif (GCC_X86 && ! FORCE_C_ONLY)

static UINT32 LOAD_UINT32_REVERSED(void *ptr)
{
    UINT32 temp;
    asm volatile("bswap %0" : "=r" (temp) : "0" (*(UINT32 *)ptr)); 
    return temp;
}

static void STORE_UINT32_REVERSED(void *ptr, UINT32 x)
{
    asm volatile("bswap %0" : "=r" (*(UINT32 *)ptr) : "0" (x)); 
}

#else

static UINT32 LOAD_UINT32_REVERSED(void *ptr)
{
    UINT32 temp = *(UINT32 *)ptr;
    temp = (temp >> 24) | ((temp & 0x00FF0000) >> 8 )
         | ((temp & 0x0000FF00) << 8 ) | (temp << 24);
    return (UINT32)temp;
}
               
static void STORE_UINT32_REVERSED(void *ptr, UINT32 x)
{
    UINT32 i = (UINT32)x;
    *(UINT32 *)ptr = (i >> 24) | ((i & 0x00FF0000) >> 8 )
                   | ((i & 0x0000FF00) << 8 ) | (i << 24);
}

#endif

/* The following definitions use the above reversal-primitives to do the right
 * thing on endian specific load and stores.
 */

#if (__LITTLE_ENDIAN__)
#define LOAD_UINT32_LITTLE(ptr)     (*(UINT32 *)(ptr))
#define STORE_UINT32_BIG(ptr,x)     STORE_UINT32_REVERSED(ptr,x)
#else
#define LOAD_UINT32_LITTLE(ptr)     LOAD_UINT32_REVERSED(ptr)
#define STORE_UINT32_BIG(ptr,x)     (*(UINT32 *)(ptr) = (UINT32)(x))
#endif



/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Begin KDF & PDF Section ---------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* UMAC uses AES with 16 byte block and key lengths */
#define AES_BLOCK_LEN  16

#if GLADMAN_AES
typedef aes_encrypt_ctx    aes_int_key[1]; /* AES internal */

#define aes_encryption(in,out,int_key) \
	    aes_encrypt((in),(out),(int_key))
#define aes_key_setup(key,int_key) \
	    aes_encrypt_key128((key),(int_key))
#else
#define AES_ROUNDS	   ((UMAC_KEY_LEN / 4) + 6)
typedef UINT8          aes_int_key[AES_ROUNDS+1][4][4]; /* AES internal */
#define aes_encryption(in,out,int_key) \
	    rijndaelEncrypt((u32 *)(int_key), AES_ROUNDS, (u8 *)(in), (u8 *)(out))
#define aes_key_setup(key,int_key) \
	    rijndaelKeySetupEnc((u32 *)(int_key), (const unsigned char *)(key), \
	    UMAC_KEY_LEN*8)
#endif

/* The user-supplied UMAC key is stretched using AES in a counter
 * mode to supply all random bits needed by UMAC. The kdf function takes
 * an AES internal key representation 'key' and writes a stream of
 * 'nbytes' bytes to the memory pointed at by 'buffer_ptr'. Each distinct
 * 'index' causes a distinct byte stream.
 */
void kdf(void *buffer_ptr, aes_int_key key, UINT8 index, int nbytes)
{
    UINT8 in_buf[AES_BLOCK_LEN] = {0};
    UINT8 out_buf[AES_BLOCK_LEN];
    UINT8 *dst_buf = (UINT8 *)buffer_ptr;
    int i;
    
    /* Setup the initial value */
    in_buf[AES_BLOCK_LEN-9] = index;
    in_buf[AES_BLOCK_LEN-1] = i = 1;
        
    while (nbytes >= AES_BLOCK_LEN) {
        aes_encryption(in_buf, out_buf, key);
        memcpy(dst_buf,out_buf,AES_BLOCK_LEN);
        in_buf[AES_BLOCK_LEN-1] = ++i;
        nbytes -= AES_BLOCK_LEN;
        dst_buf += AES_BLOCK_LEN;
    }
    if (nbytes) {
        aes_encryption(in_buf, out_buf, key);
        memcpy(dst_buf,out_buf,nbytes);
    }
}

/* The final UHASH result is XOR'd with the output of a pseudorandom
 * function. Here, we use AES to generate random output and 
 * xor the appropriate bytes depending on the last bits of nonce.
 * This scheme is optimized for sequential, increasing big-endian nonces.
 */

typedef struct {
    UINT8 cache[AES_BLOCK_LEN];  /* Previous AES output is saved      */
    UINT8 nonce[AES_BLOCK_LEN];  /* The AES input making above cache  */
    aes_int_key prf_key;         /* Expanded AES key for PDF          */
} pdf_ctx;

static void pdf_init(pdf_ctx *pc, aes_int_key prf_key)
{
    UINT8 buf[UMAC_KEY_LEN];
    
    kdf(buf, prf_key, 0, UMAC_KEY_LEN);
    aes_key_setup(buf, pc->prf_key);
    
    /* Initialize pdf and cache */
    memset(pc->nonce, 0, sizeof(pc->nonce));
    aes_encryption(pc->nonce, pc->cache, pc->prf_key);
}

static void pdf_gen_xor(pdf_ctx *pc, UINT8 nonce[8], UINT8 buf[8])
{
    /* 'index' indicates that we'll be using the 0th or 1st eight bytes
     * of the AES output. If last time around we returned the index-1st
     * element, then we may have the result in the cache already.
     */
     
#if (UMAC_OUTPUT_LEN == 4)
#define LOW_BIT_MASK 3
#elif (UMAC_OUTPUT_LEN == 8)
#define LOW_BIT_MASK 1
#elif (UMAC_OUTPUT_LEN > 8)
#define LOW_BIT_MASK 0
#endif

    UINT8 tmp_nonce_lo[4] = {0, 0, 0, 0};
    int index = nonce[7] & LOW_BIT_MASK;
    *(UINT32 *)tmp_nonce_lo = ((UINT32 *)nonce)[1];
    tmp_nonce_lo[3] &= ~LOW_BIT_MASK; /* zero last bit */

    if ( (((UINT32 *)tmp_nonce_lo)[0] != ((UINT32 *)pc->nonce)[1]) ||
         (((UINT32 *)nonce)[0] != ((UINT32 *)pc->nonce)[0]) )
    {
        ((UINT32 *)pc->nonce)[0] = ((UINT32 *)nonce)[0];
        ((UINT32 *)pc->nonce)[1] = ((UINT32 *)tmp_nonce_lo)[0];
        aes_encryption(pc->nonce, pc->cache, pc->prf_key);
    }
    
#if (UMAC_OUTPUT_LEN == 4)
    *((UINT32 *)buf) ^= ((UINT32 *)pc->cache)[index];
#elif (UMAC_OUTPUT_LEN == 8)
    *((UINT64 *)buf) ^= ((UINT64 *)pc->cache)[index];
#elif (UMAC_OUTPUT_LEN == 12)
    ((UINT64 *)buf)[0] ^= ((UINT64 *)pc->cache)[0];
    ((UINT32 *)buf)[2] ^= ((UINT32 *)pc->cache)[2];
#elif (UMAC_OUTPUT_LEN == 16)
    ((UINT64 *)buf)[0] ^= ((UINT64 *)pc->cache)[0];
    ((UINT64 *)buf)[1] ^= ((UINT64 *)pc->cache)[1];
#endif
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Begin NH Hash Section ------------------------------------------ */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* The NH-based hash functions used in UMAC are described in the UMAC paper
 * and specification, both of which can be found at the UMAC website.     
 * The interface to this implementation has two         
 * versions, one expects the entire message being hashed to be passed
 * in a single buffer and returns the hash result immediately. The second
 * allows the message to be passed in a sequence of buffers. In the          
 * muliple-buffer interface, the client calls the routine nh_update() as     
 * many times as necessary. When there is no more data to be fed to the   
 * hash, the client calls nh_final() which calculates the hash output.    
 * Before beginning another hash calculation the nh_reset() routine       
 * must be called. The single-buffer routine, nh(), is equivalent to  
 * the sequence of calls nh_update() and nh_final(); however it is        
 * optimized and should be prefered whenever the multiple-buffer interface
 * is not necessary. When using either interface, it is the client's         
 * responsability to pass no more than L1_KEY_LEN bytes per hash result.            
 *                                                                        
 * The routine nh_init() initializes the nh_ctx data structure and        
 * must be called once, before any other PDF routine.                     
 */
 
 /* The "nh_aux" routines do the actual NH hashing work. They
  * expect buffers to be multiples of L1_PAD_BOUNDARY. These routines
  * produce output for all STREAMS NH iterations in one call, 
  * allowing the parallel implementation of the streams.
  */

#define STREAMS (UMAC_OUTPUT_LEN / 4) /* Number of times hash is applied  */
#define L1_KEY_LEN         1024     /* Internal key bytes                 */
#define L1_KEY_SHIFT         16     /* Toeplitz key shift between streams */
#define L1_PAD_BOUNDARY      32     /* pad message to boundary multiple   */
#define ALLOC_BOUNDARY       16     /* Keep buffers aligned to this       */
#define HASH_BUF_BYTES       64     /* nh_aux_hb buffer multiple          */

typedef struct {
    UINT8  nh_key [L1_KEY_LEN + L1_KEY_SHIFT * (STREAMS - 1)]; /* NH Key */
    UINT8  data   [HASH_BUF_BYTES];    /* Incomming data buffer           */
    int next_data_empty;    /* Bookeeping variable for data buffer.       */
    int bytes_hashed;        /* Bytes (out of L1_KEY_LEN) incorperated.   */
    UINT64 state[STREAMS];               /* on-line state     */
} nh_ctx;



/* ---------------------------------------------------------------------- */
#if ( ! FORCE_C_ONLY && ( GCC_X86 || MSC_X86 ) )
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
#if ( SSE2 )
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
#if ( MSC_X86 )
/* ---------------------------------------------------------------------- */

/* This macro uses movdqa which requires 16-byte aligned data and key. */
#define NH_STEP_1(n) \
    movdqa xmm2, n[ecx] \
    __asm movdqa xmm0, n[eax] \
    __asm movdqa xmm3, n+16[ecx] \
    __asm movdqa xmm1, n+16[eax] \
    __asm paddd xmm2, xmm0 \
    __asm paddd xmm3, xmm1 \
    __asm movdqa xmm5, xmm2 \
    __asm pmuludq xmm2, xmm3 \
    __asm psrldq xmm3, 4 \
    __asm paddq xmm6, xmm2 \
    __asm psrldq xmm5, 4 \
    __asm pmuludq xmm3, xmm5 \
    __asm paddq xmm6, xmm3

static void nh_aux_1(void *kp, void *dp, void *hp, UINT32 dlen)
{
    __asm{
        mov edx, dlen
        mov ebx, hp
        mov ecx, kp
        mov eax, dp
        sub edx, 128
        movq xmm6, mmword ptr [ebx]
        jb label2
label1:
        NH_STEP_1(0)
        NH_STEP_1(32)
        NH_STEP_1(64)
        NH_STEP_1(96)
        add eax, 128
        add ecx, 128
        sub edx, 128
        jnb label1
label2:
        add edx,128
        je label4
label3:
        NH_STEP_1(0)
        add eax, 32
        add ecx, 32
        sub edx, 32
        jne label3
label4:
        movdqa xmm0,xmm6
        psrldq xmm0, 8
        paddq xmm6, xmm0
        movq mmword ptr [ebx], xmm6
    }
}


/* This macro uses movdqa which requires 16-byte aligned data and key. */
#define NH_STEP_2(n) \
    movdqa xmm0, n[eax] \
    __asm movdqa xmm3, n+16[ecx] \
    __asm movdqa xmm1, n+16[eax] \
    __asm paddd xmm2, xmm0 \
    __asm movdqa xmm4, xmm3 \
    __asm paddd xmm3, xmm1 \
    __asm movdqa xmm5, xmm2 \
    __asm pmuludq xmm2, xmm3 \
    __asm psrldq xmm3, 4 \
    __asm paddq xmm6, xmm2 \
    __asm movdqa xmm2, n+32[ecx] \
    __asm psrldq xmm5, 4 \
    __asm pmuludq xmm3, xmm5 \
    __asm paddd xmm1, xmm2 \
    __asm paddd xmm4, xmm0 \
    __asm paddq xmm6, xmm3 \
    __asm movdqa xmm3, xmm4 \
    __asm pmuludq xmm4, xmm1 \
    __asm psrldq xmm1, 4 \
    __asm psrldq xmm3, 4 \
    __asm pmuludq xmm3, xmm1 \
    __asm paddq xmm7, xmm4 \
    __asm paddq xmm7, xmm3


static void nh_aux_2(void *kp, void *dp, void *hp, UINT32 dlen)
/* Perform 2 streams simultaneously */
{
    __asm{
        mov edx, dlen
        mov ebx, hp
        mov ecx, kp
        mov eax, dp
        sub edx, 128
        movq xmm6, mmword ptr [ebx]
        movq xmm7, mmword ptr 8[ebx]
        movdqa xmm2, [ecx]
        jb label2
label1:
        NH_STEP_2(0)
        NH_STEP_2(32)
        NH_STEP_2(64)
        NH_STEP_2(96)
        add eax, 128
        add ecx, 128
        sub edx, 128
        jnb label1
label2:
        add edx,128
        je label4
label3:
        NH_STEP_2(0)
        add eax, 32
        add ecx, 32
        sub edx, 32
        jne label3
label4:
        movdqa xmm0,xmm6
        movdqa xmm1,xmm7
        psrldq xmm0, 8
        psrldq xmm1, 8
        paddq xmm6, xmm0
        paddq xmm7, xmm1
        movq mmword ptr [ebx], xmm6
        movq mmword ptr 8[ebx], xmm7
    }
}

/* ---------------------------------------------------------------------- */
#elif (GCC_X86)
/* ---------------------------------------------------------------------- */

#define NH_STEP_1(n) \
    "movdqa "#n"(%0), %%xmm2\n\t" \
    "movdqa "#n"(%1), %%xmm0\n\t" \
    "movdqa "#n"+16(%0), %%xmm3\n\t" \
    "movdqa "#n"+16(%1), %%xmm1\n\t" \
    "paddd %%xmm0, %%xmm2\n\t" \
    "paddd %%xmm1, %%xmm3\n\t" \
    "movdqa %%xmm2, %%xmm5\n\t" \
    "pmuludq %%xmm3, %%xmm2\n\t" \
    "psrldq $4, %%xmm3\n\t" \
    "paddq %%xmm2, %%xmm6\n\t" \
    "psrldq $4, %%xmm5\n\t" \
    "pmuludq %%xmm5, %%xmm3\n\t" \
    "paddq %%xmm3, %%xmm6\n\t"

static void nh_aux_1(void *kp, void *dp, void *hp, UINT32 dlen)
{
  UINT32 d1,d2,d3;
  asm volatile (
        "sub $128, %2\n\t"
        "movq (%3), %%xmm6\n\t"
        "jb 2f\n\t"
		".align 4,0x90\n"
	"1:\n\t"
        NH_STEP_1(0)
        NH_STEP_1(32)
        NH_STEP_1(64)
        NH_STEP_1(96)
        "add $128, %1\n\t"
        "add $128, %0\n\t"
        "sub $128, %2\n\t"
        "jnb 1b\n\t"
    	".align 4,0x90\n"
    "2:\n\t"
        "add $128, %2\n\t"
        "je 4f\n\t"
    	".align 4,0x90\n"
    "3:\n\t"
        NH_STEP_1(0)
        "add $32, %1\n\t"
        "add $32, %0\n\t"
        "sub $32, %2\n\t"
        "jne 3b\n\t"
    	".align 4,0x90\n"
    "4:\n\t"
        "movdqa %%xmm6, %%xmm0\n\t"
        "psrldq $8, %%xmm0\n\t"
        "paddq %%xmm0, %%xmm6\n\t"
        "movq %%xmm6, (%3)"
    : "+r" (kp), "+r" (dp), "+r" (dlen) 
    : "r" (hp)
    : "memory");
}

#define NH_STEP_2(n) \
    "movdqa "#n"(%1), %%xmm0\n\t" \
    "movdqa "#n"+16(%0), %%xmm3\n\t" \
    "movdqa "#n"+16(%1), %%xmm1\n\t" \
    "paddd %%xmm0, %%xmm2\n\t" \
    "movdqa %%xmm3, %%xmm4\n\t" \
    "paddd %%xmm1, %%xmm3\n\t" \
    "movdqa %%xmm2, %%xmm5\n\t" \
    "pmuludq %%xmm3, %%xmm2\n\t" \
    "psrldq $4, %%xmm3\n\t" \
    "paddq %%xmm2, %%xmm6\n\t" \
    "movdqa "#n"+32(%0), %%xmm2\n\t" \
    "psrldq $4, %%xmm5\n\t" \
    "pmuludq %%xmm5, %%xmm3\n\t" \
    "paddd %%xmm2, %%xmm1\n\t" \
    "paddd %%xmm0, %%xmm4\n\t" \
    "paddq %%xmm3, %%xmm6\n\t" \
    "movdqa %%xmm4, %%xmm3\n\t" \
    "pmuludq %%xmm1, %%xmm4\n\t" \
    "psrldq $4, %%xmm1\n\t" \
    "psrldq $4, %%xmm3\n\t" \
    "pmuludq %%xmm1, %%xmm3\n\t" \
    "paddq %%xmm4, %%xmm7\n\t" \
    "paddq %%xmm3, %%xmm7\n\t"


static void nh_aux_2(void *kp, void *dp, void *hp, UINT32 dlen)
{
  UINT32 d1,d2,d3;
  asm volatile (
        "sub $128, %2\n\t"
        "movq (%3), %%xmm6\n\t"
        "movq 8(%3), %%xmm7\n\t"
        "movdqa (%0), %%xmm2\n\t"
        "jb 2f\n\t"
		".align 4,0x90\n"
	"1:\n\t"
        NH_STEP_2(0)
        NH_STEP_2(32)
        NH_STEP_2(64)
        NH_STEP_2(96)
        "add $128, %1\n\t"
        "add $128, %0\n\t"
        "sub $128, %2\n\t"
        "jnb 1b\n\t"
    	".align 4,0x90\n"
    "2:\n\t"
        "add $128, %2\n\t"
        "je 4f\n\t"
    	".align 4,0x90\n"
    "3:\n\t"
        NH_STEP_2(0)
        "add $32, %1\n\t"
        "add $32, %0\n\t"
        "sub $32, %2\n\t"
        "jne 3b\n\t"
    	".align 4,0x90\n"
    "4:\n\t"
        "movdqa %%xmm6, %%xmm0\n\t"
        "movdqa %%xmm7, %%xmm1\n\t"
        "psrldq $8, %%xmm0\n\t"
        "psrldq $8, %%xmm1\n\t"
        "paddq %%xmm0, %%xmm6\n\t"
        "paddq %%xmm1, %%xmm7\n\t"
        "movq %%xmm6, (%3)\n\t"
        "movq %%xmm7, 8(%3)"
    : "+r" (kp), "+r" (dp), "+r" (dlen) 
    : "r" (hp)
    : "memory");
}

/* ---------------------------------------------------------------------- */
#endif /*  MSC GCC Sections for SSE2, not C */
/* ---------------------------------------------------------------------- */

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* NH hashing primitive. 128 bits are written at hp by performing two     */
/* passes over the data with the second key being the toeplitz shift of   */
/* the first.                                                             */
{
#if (UMAC_OUTPUT_LEN == 4)
    nh_aux_1(kp,dp,hp,dlen);
#elif (UMAC_OUTPUT_LEN == 8)
    nh_aux_2(kp,dp,hp,dlen);
#elif (UMAC_OUTPUT_LEN == 12)
    nh_aux_2(kp,dp,hp,dlen);
    nh_aux_1((UINT8 *)kp+32,dp,(UINT8 *)hp+16,dlen);
#elif (UMAC_OUTPUT_LEN == 16)
    nh_aux_2(kp,dp,hp,dlen);
    nh_aux_2((UINT8 *)kp+32,dp,(UINT8 *)hp+16,dlen);
#endif
}



/* ---------------------------------------------------------------------- */
#else /* not SSE2 */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
#if ( MSC_X86 )
/* ---------------------------------------------------------------------- */

#define NH_STEP(n) \
    mov eax,n[ebx]   \
    __asm mov edx,n+16[ebx] \
    __asm add eax,n[ecx]   \
    __asm add edx,n+16[ecx] \
    __asm mul edx         \
    __asm add esi,eax      \
    __asm adc edi,edx


static void nh_aux_1(void *kp, void *dp, void *hp, UINT32 dlen)
{
  __asm{
      push ebp
      mov ecx,kp
      mov ebx,dp
      mov eax,hp
      mov ebp,dlen
      sub ebp,128
      mov esi,[eax]
      mov edi,4[eax]
      jb label2     /* if 0 */
label1:
      NH_STEP(0)
      NH_STEP(4)
      NH_STEP(8)
      NH_STEP(12)
      NH_STEP(32)
      NH_STEP(36)
      NH_STEP(40)
      NH_STEP(44)
      NH_STEP(64)
      NH_STEP(68)
      NH_STEP(72)
      NH_STEP(76)
      NH_STEP(96)
      NH_STEP(100)
      NH_STEP(104)
      NH_STEP(108)
      add ecx,128
      add ebx,128
      sub ebp,128
      jnb label1
label2:
      add ebp,128
      je label4
label3:
      NH_STEP(0)
      NH_STEP(4)
      NH_STEP(8)
      NH_STEP(12)
      add ecx,32
      add ebx,32
      sub ebp,32
      jne label3
label4:
      pop ebp
      mov eax,hp
      mov [eax],esi
      mov 4[eax],edi
   }
}

/* ---------------------------------------------------------------------- */
#elif ( GCC_X86 )
/* ---------------------------------------------------------------------- */

#define NH_STEP(n) \
    "movl "#n"(%%ebx),%%eax\n\t" \
    "movl "#n"+16(%%ebx),%%edx\n\t" \
    "addl "#n"(%%ecx),%%eax\n\t" \
    "addl "#n"+16(%%ecx),%%edx\n\t" \
    "mull %%edx\n\t" \
    "addl %%eax,%%esi\n\t" \
    "adcl %%edx,%%edi\n\t"
    
static void nh_aux_1(void *kp, void *dp, void *hp, UINT32 dlen)
/* NH hashing primitive. Previous (partial) hash result is loaded and     */
/* then stored via hp pointer. The length of the data pointed at by dp is */
/* guaranteed to be divisible by HASH_BUF_BYTES (64), which means we can   */
/* optimize by unrolling the loop. 64 bits are written at hp.             */
{
  UINT32 *p = (UINT32 *)hp;
  
  asm volatile (
    "\n\t"
    "pushl %%eax\n\t"
    "pushl %%ebp\n\t"
    "subl $128,%%eax\n\t"
    "movl %%eax,%%ebp\n\t"
    "jb 2f\n\t"
    ".align 4,0x90\n"
    "1:\n\t"
    
    NH_STEP(0)
    NH_STEP(4)
    NH_STEP(8)
    NH_STEP(12)
    NH_STEP(32)
    NH_STEP(36)
    NH_STEP(40)
    NH_STEP(44)
    NH_STEP(64)
    NH_STEP(68)
    NH_STEP(72)
    NH_STEP(76)
    NH_STEP(96)
    NH_STEP(100)
    NH_STEP(104)
    NH_STEP(108)

    "addl $128,%%ecx\n\t"
    "addl $128,%%ebx\n\t"
    "subl $128,%%ebp\n\t"
    "jnb 1b\n\t"
    ".align 4\n"
    "2:\n\t"
    "addl $128,%%ebp\n\t"
    "je 4f\n\t"
    ".align 4,0x90\n"
    "3:\n\t"
    
    NH_STEP(0)
    NH_STEP(4)
    NH_STEP(8)
    NH_STEP(12)

    "addl $32,%%ecx\n\t"
    "addl $32,%%ebx\n\t"
    "subl $32,%%ebp\n\t"
    "jne 3b\n\t"
    ".align 4\n"
    "4:\n\t"
    "popl %%ebp\n\t"
    "popl %%eax"
    : "+S" (p[0]), "+D" (p[1]), "+c" (kp), "+b" (dp)
    : "a" (dlen)
    : "edx", "memory");
}

/* ---------------------------------------------------------------------- */
#endif /* GCC or MSC, not SSE2, not C */
/* ---------------------------------------------------------------------- */

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* NH hashing primitive. 128 bits are written at hp by performing two     */
/* passes over the data with the second key being the toeplitz shift of   */
/* the first.                                                             */
{
    nh_aux_1(kp,dp,hp,dlen);
#if (UMAC_OUTPUT_LEN >= 8)
    nh_aux_1((UINT8 *)kp+16,dp,(UINT8 *)hp+8,dlen);
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    nh_aux_1((UINT8 *)kp+32,dp,(UINT8 *)hp+16,dlen);
#endif
#if (UMAC_OUTPUT_LEN == 16)
    nh_aux_1((UINT8 *)kp+48,dp,(UINT8 *)hp+24,dlen);
#endif
}

/* ---------------------------------------------------------------------- */
#endif /* SSE2 */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
#else /* FORCE_C_ONLY */
/* ---------------------------------------------------------------------- */

#if (UMAC_OUTPUT_LEN == 4)

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* NH hashing primitive. Previous (partial) hash result is loaded and     
* then stored via hp pointer. The length of the data pointed at by "dp",
* "dlen", is guaranteed to be divisible by L1_PAD_BOUNDARY (32).  Key
* is expected to be endian compensated in memory at key setup.    
*/
{
    UINT64 h;
    UWORD c = dlen / 32;
    UINT32 *k = (UINT32 *)kp;
    UINT32 *d = (UINT32 *)dp;
    UINT32 d0,d1,d2,d3,d4,d5,d6,d7;
    UINT32 k0,k1,k2,k3,k4,k5,k6,k7;
    
    h = *((UINT64 *)hp);
    do {
        d0 = LOAD_UINT32_LITTLE(d+0); d1 = LOAD_UINT32_LITTLE(d+1);
        d2 = LOAD_UINT32_LITTLE(d+2); d3 = LOAD_UINT32_LITTLE(d+3);
        d4 = LOAD_UINT32_LITTLE(d+4); d5 = LOAD_UINT32_LITTLE(d+5);
        d6 = LOAD_UINT32_LITTLE(d+6); d7 = LOAD_UINT32_LITTLE(d+7);
        k0 = *(k+0); k1 = *(k+1); k2 = *(k+2); k3 = *(k+3);
        k4 = *(k+4); k5 = *(k+5); k6 = *(k+6); k7 = *(k+7);
        h += MUL64((k0 + d0), (k4 + d4));
        h += MUL64((k1 + d1), (k5 + d5));
        h += MUL64((k2 + d2), (k6 + d6));
        h += MUL64((k3 + d3), (k7 + d7));
        
        d += 8;
        k += 8;
    } while (--c);
  *((UINT64 *)hp) = h;
}

#elif (UMAC_OUTPUT_LEN == 8)

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* Same as previous nh_aux, but two streams are handled in one pass,
 * reading and writing 16 bytes of hash-state per call.
 */
{
  UINT64 h1,h2;
  UWORD c = dlen / 32;
  UINT32 *k = (UINT32 *)kp;
  UINT32 *d = (UINT32 *)dp;
  UINT32 d0,d1,d2,d3,d4,d5,d6,d7;
  UINT32 k0,k1,k2,k3,k4,k5,k6,k7,
        k8,k9,k10,k11;

  h1 = *((UINT64 *)hp);
  h2 = *((UINT64 *)hp + 1);
  k0 = *(k+0); k1 = *(k+1); k2 = *(k+2); k3 = *(k+3);
  do {
    d0 = LOAD_UINT32_LITTLE(d+0); d1 = LOAD_UINT32_LITTLE(d+1);
    d2 = LOAD_UINT32_LITTLE(d+2); d3 = LOAD_UINT32_LITTLE(d+3);
    d4 = LOAD_UINT32_LITTLE(d+4); d5 = LOAD_UINT32_LITTLE(d+5);
    d6 = LOAD_UINT32_LITTLE(d+6); d7 = LOAD_UINT32_LITTLE(d+7);
    k4 = *(k+4); k5 = *(k+5); k6 = *(k+6); k7 = *(k+7);
    k8 = *(k+8); k9 = *(k+9); k10 = *(k+10); k11 = *(k+11);

    h1 += MUL64((k0 + d0), (k4 + d4));
    h2 += MUL64((k4 + d0), (k8 + d4));

    h1 += MUL64((k1 + d1), (k5 + d5));
    h2 += MUL64((k5 + d1), (k9 + d5));

    h1 += MUL64((k2 + d2), (k6 + d6));
    h2 += MUL64((k6 + d2), (k10 + d6));

    h1 += MUL64((k3 + d3), (k7 + d7));
    h2 += MUL64((k7 + d3), (k11 + d7));

    k0 = k8; k1 = k9; k2 = k10; k3 = k11;

    d += 8;
    k += 8;
  } while (--c);
  ((UINT64 *)hp)[0] = h1;
  ((UINT64 *)hp)[1] = h2;
}

#elif (UMAC_OUTPUT_LEN == 12)

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* Same as previous nh_aux, but two streams are handled in one pass,
 * reading and writing 24 bytes of hash-state per call.
*/
{
    UINT64 h1,h2,h3;
    UWORD c = dlen / 32;
    UINT32 *k = (UINT32 *)kp;
    UINT32 *d = (UINT32 *)dp;
    UINT32 d0,d1,d2,d3,d4,d5,d6,d7;
    UINT32 k0,k1,k2,k3,k4,k5,k6,k7,
        k8,k9,k10,k11,k12,k13,k14,k15;
    
    h1 = *((UINT64 *)hp);
    h2 = *((UINT64 *)hp + 1);
    h3 = *((UINT64 *)hp + 2);
    k0 = *(k+0); k1 = *(k+1); k2 = *(k+2); k3 = *(k+3);
    k4 = *(k+4); k5 = *(k+5); k6 = *(k+6); k7 = *(k+7);
    do {
        d0 = LOAD_UINT32_LITTLE(d+0); d1 = LOAD_UINT32_LITTLE(d+1);
        d2 = LOAD_UINT32_LITTLE(d+2); d3 = LOAD_UINT32_LITTLE(d+3);
        d4 = LOAD_UINT32_LITTLE(d+4); d5 = LOAD_UINT32_LITTLE(d+5);
        d6 = LOAD_UINT32_LITTLE(d+6); d7 = LOAD_UINT32_LITTLE(d+7);
        k8 = *(k+8); k9 = *(k+9); k10 = *(k+10); k11 = *(k+11);
        k12 = *(k+12); k13 = *(k+13); k14 = *(k+14); k15 = *(k+15);
        
        h1 += MUL64((k0 + d0), (k4 + d4));
        h2 += MUL64((k4 + d0), (k8 + d4));
        h3 += MUL64((k8 + d0), (k12 + d4));
        
        h1 += MUL64((k1 + d1), (k5 + d5));
        h2 += MUL64((k5 + d1), (k9 + d5));
        h3 += MUL64((k9 + d1), (k13 + d5));
        
        h1 += MUL64((k2 + d2), (k6 + d6));
        h2 += MUL64((k6 + d2), (k10 + d6));
        h3 += MUL64((k10 + d2), (k14 + d6));
        
        h1 += MUL64((k3 + d3), (k7 + d7));
        h2 += MUL64((k7 + d3), (k11 + d7));
        h3 += MUL64((k11 + d3), (k15 + d7));
        
        k0 = k8; k1 = k9; k2 = k10; k3 = k11;
        k4 = k12; k5 = k13; k6 = k14; k7 = k15;
        
        d += 8;
        k += 8;
    } while (--c);
    ((UINT64 *)hp)[0] = h1;
    ((UINT64 *)hp)[1] = h2;
    ((UINT64 *)hp)[2] = h3;
}

#elif (UMAC_OUTPUT_LEN == 16)

static void nh_aux(void *kp, void *dp, void *hp, UINT32 dlen)
/* Same as previous nh_aux, but two streams are handled in one pass,
 * reading and writing 24 bytes of hash-state per call.
*/
{
    UINT64 h1,h2,h3,h4;
    UWORD c = dlen / 32;
    UINT32 *k = (UINT32 *)kp;
    UINT32 *d = (UINT32 *)dp;
    UINT32 d0,d1,d2,d3,d4,d5,d6,d7;
    UINT32 k0,k1,k2,k3,k4,k5,k6,k7,
        k8,k9,k10,k11,k12,k13,k14,k15,
        k16,k17,k18,k19;
    
    h1 = *((UINT64 *)hp);
    h2 = *((UINT64 *)hp + 1);
    h3 = *((UINT64 *)hp + 2);
    h4 = *((UINT64 *)hp + 3);
    k0 = *(k+0); k1 = *(k+1); k2 = *(k+2); k3 = *(k+3);
    k4 = *(k+4); k5 = *(k+5); k6 = *(k+6); k7 = *(k+7);
    do {
        d0 = LOAD_UINT32_LITTLE(d+0); d1 = LOAD_UINT32_LITTLE(d+1);
        d2 = LOAD_UINT32_LITTLE(d+2); d3 = LOAD_UINT32_LITTLE(d+3);
        d4 = LOAD_UINT32_LITTLE(d+4); d5 = LOAD_UINT32_LITTLE(d+5);
        d6 = LOAD_UINT32_LITTLE(d+6); d7 = LOAD_UINT32_LITTLE(d+7);
        k8 = *(k+8); k9 = *(k+9); k10 = *(k+10); k11 = *(k+11);
        k12 = *(k+12); k13 = *(k+13); k14 = *(k+14); k15 = *(k+15);
        k16 = *(k+16); k17 = *(k+17); k18 = *(k+18); k19 = *(k+19);
        
        h1 += MUL64((k0 + d0), (k4 + d4));
        h2 += MUL64((k4 + d0), (k8 + d4));
        h3 += MUL64((k8 + d0), (k12 + d4));
        h4 += MUL64((k12 + d0), (k16 + d4));
        
        h1 += MUL64((k1 + d1), (k5 + d5));
        h2 += MUL64((k5 + d1), (k9 + d5));
        h3 += MUL64((k9 + d1), (k13 + d5));
        h4 += MUL64((k13 + d1), (k17 + d5));
        
        h1 += MUL64((k2 + d2), (k6 + d6));
        h2 += MUL64((k6 + d2), (k10 + d6));
        h3 += MUL64((k10 + d2), (k14 + d6));
        h4 += MUL64((k14 + d2), (k18 + d6));
        
        h1 += MUL64((k3 + d3), (k7 + d7));
        h2 += MUL64((k7 + d3), (k11 + d7));
        h3 += MUL64((k11 + d3), (k15 + d7));
        h4 += MUL64((k15 + d3), (k19 + d7));
        
        k0 = k8; k1 = k9; k2 = k10; k3 = k11;
        k4 = k12; k5 = k13; k6 = k14; k7 = k15;
        k8 = k16; k9 = k17; k10 = k18; k11 = k19;
        
        d += 8;
        k += 8;
    } while (--c);
    ((UINT64 *)hp)[0] = h1;
    ((UINT64 *)hp)[1] = h2;
    ((UINT64 *)hp)[2] = h3;
    ((UINT64 *)hp)[3] = h4;
}

/* ---------------------------------------------------------------------- */
#endif  /* UMAC_OUTPUT_LENGTH */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
#endif  /* FORCE_C_ONLY */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */

static void nh_transform(nh_ctx *hc, UINT8 *buf, UINT32 nbytes)
/* This function is a wrapper for the primitive NH hash functions. It takes
 * as argument "hc" the current hash context and a buffer which must be a
 * multiple of L1_PAD_BOUNDARY. The key passed to nh_aux is offset
 * appropriately according to how much message has been hashed already.
 */
{
    UINT8 *key;
  
    key = hc->nh_key + hc->bytes_hashed;
    nh_aux(key, buf, hc->state, nbytes);
}

/* ---------------------------------------------------------------------- */

static void endian_convert(void *buf, UWORD bpw, UINT32 num_bytes)
/* We endian convert the keys on little-endian computers to               */
/* compensate for the lack of big-endian memory reads during hashing.     */
{
    UWORD iters = num_bytes / bpw;
    if (bpw == 4) {
        UINT32 *p = (UINT32 *)buf;
        do {
            *p = LOAD_UINT32_REVERSED(p);
            p++;
        } while (--iters);
    } else if (bpw == 8) {
        UINT32 *p = (UINT32 *)buf;
        UINT32 t;
        do {
            t = LOAD_UINT32_REVERSED(p+1);
            p[1] = LOAD_UINT32_REVERSED(p);
            p[0] = t;
            p += 2;
        } while (--iters);
    }
}
#if (__LITTLE_ENDIAN__)
#define endian_convert_if_le(x,y,z) endian_convert((x),(y),(z))
#else
#define endian_convert_if_le(x,y,z) do{}while(0)  /* Do nothing */
#endif

/* ---------------------------------------------------------------------- */

static void nh_reset(nh_ctx *hc)
/* Reset nh_ctx to ready for hashing of new data */
{
    hc->bytes_hashed = 0;
    hc->next_data_empty = 0;
    hc->state[0] = 0;
#if (UMAC_OUTPUT_LEN >= 8)
    hc->state[1] = 0;
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    hc->state[2] = 0;
#endif
#if (UMAC_OUTPUT_LEN == 16)
    hc->state[3] = 0;
#endif

}

/* ---------------------------------------------------------------------- */

static void nh_init(nh_ctx *hc, aes_int_key prf_key)
/* Generate nh_key, endian convert and reset to be ready for hashing.   */
{
    kdf(hc->nh_key, prf_key, 1, sizeof(hc->nh_key));
    endian_convert_if_le(hc->nh_key, 4, sizeof(hc->nh_key));
    nh_reset(hc);
}

/* ---------------------------------------------------------------------- */

static void nh_update(nh_ctx *hc, UINT8 *buf, UINT32 nbytes)
/* Incorporate nbytes of data into a nh_ctx, buffer whatever is not an    */
/* even multiple of HASH_BUF_BYTES.                                       */
{
    UINT32 i,j;
    
    j = hc->next_data_empty;
    if ((j + nbytes) >= HASH_BUF_BYTES) {
        if (j) {
            i = HASH_BUF_BYTES - j;
            memcpy(hc->data+j, buf, i);
            nh_transform(hc,hc->data,HASH_BUF_BYTES);
            nbytes -= i;
            buf += i;
            hc->bytes_hashed += HASH_BUF_BYTES;
        }
        if (nbytes >= HASH_BUF_BYTES) {
            i = nbytes & ~(HASH_BUF_BYTES - 1);
            nh_transform(hc, buf, i);
            nbytes -= i;
            buf += i;
            hc->bytes_hashed += i;
        }
        j = 0;
    }
    memcpy(hc->data + j, buf, nbytes);
    hc->next_data_empty = j + nbytes;
}

/* ---------------------------------------------------------------------- */

static void zero_pad(UINT8 *p, int nbytes)
{
/* Write "nbytes" of zeroes, beginning at "p" */
    if (nbytes >= (int)sizeof(UWORD)) {
        while ((ptrdiff_t)p % sizeof(UWORD)) {
            *p = 0;
            nbytes--;
            p++;
        }
        while (nbytes >= (int)sizeof(UWORD)) {
            *(UWORD *)p = 0;
            nbytes -= sizeof(UWORD);
            p += sizeof(UWORD);
        }
    }
    while (nbytes) {
        *p = 0;
        nbytes--;
        p++;
    }
}

/* ---------------------------------------------------------------------- */

static void nh_final(nh_ctx *hc, UINT8 *result)
/* After passing some number of data buffers to nh_update() for integration
 * into an NH context, nh_final is called to produce a hash result. If any
 * bytes are in the buffer hc->data, incorporate them into the
 * NH context. Finally, add into the NH accumulation "state" the total number
 * of bits hashed. The resulting numbers are written to the buffer "result".
 * If nh_update was never called, L1_PAD_BOUNDARY zeroes are incorporated.
 */
{
    int nh_len, nbits;

    if (hc->next_data_empty != 0) {
        nh_len = ((hc->next_data_empty + (L1_PAD_BOUNDARY - 1)) &
                                                ~(L1_PAD_BOUNDARY - 1));
        zero_pad(hc->data + hc->next_data_empty, 
                                          nh_len - hc->next_data_empty);
        nh_transform(hc, hc->data, nh_len);
        hc->bytes_hashed += hc->next_data_empty;
    } else if (hc->bytes_hashed == 0) {
    	nh_len = L1_PAD_BOUNDARY;
        zero_pad(hc->data, L1_PAD_BOUNDARY);
        nh_transform(hc, hc->data, nh_len);
    }

    nbits = (hc->bytes_hashed << 3);
    ((UINT64 *)result)[0] = ((UINT64 *)hc->state)[0] + nbits;
#if (UMAC_OUTPUT_LEN >= 8)
    ((UINT64 *)result)[1] = ((UINT64 *)hc->state)[1] + nbits;
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    ((UINT64 *)result)[2] = ((UINT64 *)hc->state)[2] + nbits;
#endif
#if (UMAC_OUTPUT_LEN == 16)
    ((UINT64 *)result)[3] = ((UINT64 *)hc->state)[3] + nbits;
#endif
    nh_reset(hc);
}

/* ---------------------------------------------------------------------- */

static void nh(nh_ctx *hc, UINT8 *buf, UINT32 padded_len,
               UINT32 unpadded_len, UINT8 *result)
/* All-in-one nh_update() and nh_final() equivalent.
 * Assumes that padded_len is divisible by L1_PAD_BOUNDARY and result is
 * well aligned
 */
{
    UINT32 nbits;
    
    /* Initialize the hash state */
    nbits = (unpadded_len << 3);
    
    ((UINT64 *)result)[0] = nbits;
#if (UMAC_OUTPUT_LEN >= 8)
    ((UINT64 *)result)[1] = nbits;
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    ((UINT64 *)result)[2] = nbits;
#endif
#if (UMAC_OUTPUT_LEN == 16)
    ((UINT64 *)result)[3] = nbits;
#endif
    
    nh_aux(hc->nh_key, buf, result, padded_len);
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Begin UHASH Section -------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* UHASH is a multi-layered algorithm. Data presented to UHASH is first
 * hashed by NH. The NH output is then hashed by a polynomial-hash layer
 * unless the initial data to be hashed is short. After the polynomial-
 * layer, an inner-product hash is used to produce the final UHASH output.
 *
 * UHASH provides two interfaces, one all-at-once and another where data
 * buffers are presented sequentially. In the sequential interface, the
 * UHASH client calls the routine uhash_update() as many times as necessary.
 * When there is no more data to be fed to UHASH, the client calls
 * uhash_final() which          
 * calculates the UHASH output. Before beginning another UHASH calculation    
 * the uhash_reset() routine must be called. The all-at-once UHASH routine,   
 * uhash(), is equivalent to the sequence of calls uhash_update() and         
 * uhash_final(); however it is optimized and should be                     
 * used whenever the sequential interface is not necessary.              
 *                                                                        
 * The routine uhash_init() initializes the uhash_ctx data structure and    
 * must be called once, before any other UHASH routine.
 */                                                        

/* ---------------------------------------------------------------------- */
/* ----- Constants and uhash_ctx ---------------------------------------- */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
/* ----- Poly hash and Inner-Product hash Constants --------------------- */
/* ---------------------------------------------------------------------- */

/* Primes and masks */
#define p36    ((UINT64)0x0000000FFFFFFFFBull)              /* 2^36 -  5 */
#define p64    ((UINT64)0xFFFFFFFFFFFFFFC5ull)              /* 2^64 - 59 */
#define m36    ((UINT64)0x0000000FFFFFFFFFull)  /* The low 36 of 64 bits */


/* ---------------------------------------------------------------------- */

typedef struct uhash_ctx {
    nh_ctx hash;                          /* Hash context for L1 NH hash  */
    UINT64 poly_key_8[STREAMS];           /* p64 poly keys                */
    UINT64 poly_accum[STREAMS];           /* poly hash result             */
    UINT64 ip_keys[STREAMS*4];            /* Inner-product keys           */
    UINT32 ip_trans[STREAMS];             /* Inner-product translation    */
    UINT32 msg_len;                       /* Total length of data passed  */
                                          /* to uhash */
} uhash_ctx;

/* ---------------------------------------------------------------------- */


/* The polynomial hashes use Horner's rule to evaluate a polynomial one
 * word at a time. As described in the specification, poly32 and poly64
 * require keys from special domains. The following impelementations exploit
 * the special domains to avoid overflow. The results are not guaranteed to
 * be within Z_p32 and Z_p64, but the Inner-Product hash implementation
 * patches any errant values.
 */

static UINT64 poly64(UINT64 cur, UINT64 key, UINT64 data)
{
    UINT32 key_hi = (UINT32)(key >> 32),
           key_lo = (UINT32)key,
           cur_hi = (UINT32)(cur >> 32),
           cur_lo = (UINT32)cur,
           x_lo,
           x_hi;
    UINT64 X,T,res;
    
    X =  MUL64(key_hi, cur_lo) + MUL64(cur_hi, key_lo);
    x_lo = (UINT32)X;
    x_hi = (UINT32)(X >> 32);
    
    res = (MUL64(key_hi, cur_hi) + x_hi) * 59 + MUL64(key_lo, cur_lo);
     
    T = ((UINT64)x_lo << 32);
    res += T;
    if (res < T)
        res += 59;

    res += data;
    if (res < data)
        res += 59;

    return res;
}


/* Although UMAC is specified to use a ramped polynomial hash scheme, this
 * impelemtation does not handle all ramp levels. Because we don't handle
 * the ramp up to p128 modulus in this implementation, we are limited to
 * 2^14 poly_hash() invocations per stream (for a total capacity of 2^24
 * bytes input to UMAC per tag, ie. 16MB).
 */
static void poly_hash(uhash_ctx_t hc, UINT32 data_in[])
{
    int i;
    UINT64 *data=(UINT64*)data_in;
    
    for (i = 0; i < STREAMS; i++) {
        if ((UINT32)(data[i] >> 32) == 0xfffffffful) {
            hc->poly_accum[i] = poly64(hc->poly_accum[i], 
                                       hc->poly_key_8[i], p64 - 1);
            hc->poly_accum[i] = poly64(hc->poly_accum[i],
                                       hc->poly_key_8[i], (data[i] - 59));
        } else {
            hc->poly_accum[i] = poly64(hc->poly_accum[i],
                                       hc->poly_key_8[i], data[i]);
        }
    }
}


/* ---------------------------------------------------------------------- */


/* The final step in UHASH is an inner-product hash. The poly hash
 * produces a result not neccesarily WORD_LEN bytes long. The inner-
 * product hash breaks the polyhash output into 16-bit chunks and
 * multiplies each with a 36 bit key.
 */

#if (MSC_X86 && ! FORCE_C_ONLY)

static UINT64 ip_aux(UINT64 t, UINT64 *ipkp, UINT64 data)
{
    UINT32 data_hi = (UINT32)(data >> 32),
           data_lo = (UINT32)(data),
           t_hi = (UINT32)(t >> 32),
           t_lo = (UINT32)(t);
    __asm{
        mov edi, ipkp
        mov ebx,data_hi
        mov ecx,data_lo
        mov esi, t_lo
        mov edx, t_hi
        push ebp
        mov ebp,edx
        mov eax,ebx
        shr eax,16
        mul DWORD PTR 0[edi]
        add esi,eax
        adc ebp,edx
        mov eax,ebx
        shr eax,16
        mul DWORD PTR 4[edi]
        add ebp,eax

        movzx eax,bx
        mul DWORD PTR 8[edi]
        add esi,eax
        adc ebp,edx
        movzx eax,bx
        mul DWORD PTR 12[edi]
        add ebp,eax

        mov eax,ecx
        shr eax,16
        mul DWORD PTR 16[edi]
        add esi,eax
        adc ebp,edx
        mov eax,ecx
        shr eax,16
        mul DWORD PTR 20[edi]
        add ebp,eax

        movzx eax,cx
        mul DWORD PTR 24[edi]
        add esi,eax
        adc ebp,edx
        movzx eax,cx
        mul DWORD PTR 28[edi]
        lea edx,[eax+ebp]
        mov eax,esi
        pop ebp
        /* MSVC returns UINT64 in edx:eax */
    }
}

static UINT32 ip_reduce_p36(UINT64 t)
{
    UINT32 t_hi = (UINT32)(t >> 32),
           t_lo = (UINT32)(t);
    __asm{
        mov edx,t_hi
        mov eax,t_lo
        mov edi,edx
        and edx,15
        shr edi,4
        lea edi,[edi+edi*4]
        add eax,edi
        adc edx,0
        cmp edx,0xf
        jb skip_sub
        ja do_sub
        cmp eax,0xfffffffb
        jb skip_sub
do_sub:
        sub eax, 0xfffffffb
        /* sbb  edx, 0xf We don't return the high word */
skip_sub:
    }
}

#elif (GCC_X86 && ! FORCE_C_ONLY)

static UINT64 ip_aux(UINT64 t, UINT64 *ipkp, UINT64 data)
{
    UINT32 dummy1, dummy2;
    asm volatile(
        "pushl %%ebp\n\t"
        "movl %%eax,%%esi\n\t"
        "movl %%edx,%%ebp\n\t"
        "movl %%ebx,%%eax\n\t"
        "shrl $16,%%eax\n\t"
        "mull 0(%%edi)\n\t"
        "addl %%eax,%%esi\n\t"
        "adcl %%edx,%%ebp\n\t"
        "movl %%ebx,%%eax\n\t"
        "shrl $16,%%eax\n\t"
        "mull 4(%%edi)\n\t"
        "addl %%eax,%%ebp\n\t"

        "movzwl %%bx,%%eax\n\t"
        "mull 8(%%edi)\n\t"
        "addl %%eax,%%esi\n\t"
        "adcl %%edx,%%ebp\n\t"
        "movzwl %%bx,%%eax\n\t"
        "mull 12(%%edi)\n\t"
        "addl %%eax,%%ebp\n\t"

        "movl %%ecx,%%eax\n\t"
        "shrl $16,%%eax\n\t"
        "mull 16(%%edi)\n\t"
        "addl %%eax,%%esi\n\t"
        "adcl %%edx,%%ebp\n\t"
        "movl %%ecx,%%eax\n\t"
        "shrl $16,%%eax\n\t"
        "mull 20(%%edi)\n\t"
        "addl %%eax,%%ebp\n\t"

        "movzwl %%cx,%%eax\n\t"
        "mull 24(%%edi)\n\t"
        "addl %%eax,%%esi\n\t"
        "adcl %%edx,%%ebp\n\t"
        "movzwl %%cx,%%eax\n\t"
        "mull 28(%%edi)\n\t"
        "leal (%%eax,%%ebp),%%edx\n\t"
        "movl %%esi,%%eax\n\t"
        "popl %%ebp"
    : "+A"(t), "=b"(dummy1), "=c"(dummy2)
    : "D"(ipkp), "1"((UINT32)(data>>32)), "2"((UINT32)data)
    : "esi");
    
    return t;
}

static UINT32 ip_reduce_p36(UINT64 t)
{
    asm volatile(
        "movl %%edx,%%edi\n\t"
        "andl $15,%%edx\n\t"
        "shrl $4,%%edi\n\t"
        "leal (%%edi,%%edi,4),%%edi\n\t"
        "addl %%edi,%%eax\n\t"
        "adcl $0,%%edx\n\t"
    : "+A"(t)
    :
    : "edi");

    if (t >= p36)
        t -= p36;

    return (UINT32)(t);
}

#else

static UINT64 ip_aux(UINT64 t, UINT64 *ipkp, UINT64 data)
{
    t = t + ipkp[0] * (UINT64)(UINT16)(data >> 48);
    t = t + ipkp[1] * (UINT64)(UINT16)(data >> 32);
    t = t + ipkp[2] * (UINT64)(UINT16)(data >> 16);
    t = t + ipkp[3] * (UINT64)(UINT16)(data);
    
    return t;
}

static UINT32 ip_reduce_p36(UINT64 t)
{
/* Divisionless modular reduction */
    UINT64 ret;
    
    ret = (t & m36) + 5 * (t >> 36);
    if (ret >= p36)
        ret -= p36;

    /* return least significant 32 bits */
    return (UINT32)(ret);
}


#endif

/* If the data being hashed by UHASH is no longer than L1_KEY_LEN, then
 * the polyhash stage is skipped and ip_short is applied directly to the
 * NH output.
 */
static void ip_short(uhash_ctx_t ahc, UINT8 *nh_res, char *res)
{
    UINT64 t;
    UINT64 *nhp = (UINT64 *)nh_res;
    
    t  = ip_aux(0,ahc->ip_keys, nhp[0]);
    STORE_UINT32_BIG((UINT32 *)res+0, ip_reduce_p36(t) ^ ahc->ip_trans[0]);
#if (UMAC_OUTPUT_LEN >= 8)
    t  = ip_aux(0,ahc->ip_keys+4, nhp[1]);
    STORE_UINT32_BIG((UINT32 *)res+1, ip_reduce_p36(t) ^ ahc->ip_trans[1]);
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    t  = ip_aux(0,ahc->ip_keys+8, nhp[2]);
    STORE_UINT32_BIG((UINT32 *)res+2, ip_reduce_p36(t) ^ ahc->ip_trans[2]);
#endif
#if (UMAC_OUTPUT_LEN == 16)
    t  = ip_aux(0,ahc->ip_keys+12, nhp[3]);
    STORE_UINT32_BIG((UINT32 *)res+3, ip_reduce_p36(t) ^ ahc->ip_trans[3]);
#endif
}

/* If the data being hashed by UHASH is longer than L1_KEY_LEN, then
 * the polyhash stage is not skipped and ip_long is applied to the
 * polyhash output.
 */
static void ip_long(uhash_ctx_t ahc, char *res)
{
    int i;
    UINT64 t;

    for (i = 0; i < STREAMS; i++) {
        /* fix polyhash output not in Z_p64 */
        if (ahc->poly_accum[i] >= p64)
            ahc->poly_accum[i] -= p64;
        t  = ip_aux(0,ahc->ip_keys+(i*4), ahc->poly_accum[i]);
        STORE_UINT32_BIG((UINT32 *)res+i, 
                         ip_reduce_p36(t) ^ ahc->ip_trans[i]);
    }
}


/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */

/* Reset uhash context for next hash session */
int uhash_reset(uhash_ctx_t pc)
{
    nh_reset(&pc->hash);
    pc->msg_len = 0;
    pc->poly_accum[0] = 1;
#if (UMAC_OUTPUT_LEN >= 8)
    pc->poly_accum[1] = 1;
#endif
#if (UMAC_OUTPUT_LEN >= 12)
    pc->poly_accum[2] = 1;
#endif
#if (UMAC_OUTPUT_LEN == 16)
    pc->poly_accum[3] = 1;
#endif
    return 1;
}

/* ---------------------------------------------------------------------- */

/* Given a pointer to the internal key needed by kdf() and a uhash context,
 * initialize the NH context and generate keys needed for poly and inner-
 * product hashing. All keys are endian adjusted in memory so that native
 * loads cause correct keys to be in registers during calculation.
 */
static void uhash_init(uhash_ctx_t ahc, aes_int_key prf_key)
{
    int i;
    UINT8 buf[(8*STREAMS+4)*sizeof(UINT64)];
    
    /* Zero the entire uhash context */
    memset(ahc, 0, sizeof(uhash_ctx));

    /* Initialize the L1 hash */
    nh_init(&ahc->hash, prf_key);
    
    /* Setup L2 hash variables */
    kdf(buf, prf_key, 2, sizeof(buf));    /* Fill buffer with index 1 key */
    for (i = 0; i < STREAMS; i++) {
        /* Fill keys from the buffer, skipping bytes in the buffer not
         * used by this implementation. Endian reverse the keys if on a
         * little-endian computer.
         */
        memcpy(ahc->poly_key_8+i, buf+24*i, 8);
        endian_convert_if_le(ahc->poly_key_8+i, 8, 8);
        /* Mask the 64-bit keys to their special domain */
        ahc->poly_key_8[i] &= ((UINT64)0x01ffffffu << 32) + 0x01ffffffu;
        ahc->poly_accum[i] = 1;  /* Our polyhash prepends a non-zero word */
    }
    
    /* Setup L3-1 hash variables */
    kdf(buf, prf_key, 3, sizeof(buf)); /* Fill buffer with index 2 key */
    for (i = 0; i < STREAMS; i++)
          memcpy(ahc->ip_keys+4*i, buf+(8*i+4)*sizeof(UINT64),
                                                 4*sizeof(UINT64));
    endian_convert_if_le(ahc->ip_keys, sizeof(UINT64), 
                                                  sizeof(ahc->ip_keys));
    for (i = 0; i < STREAMS*4; i++)
        ahc->ip_keys[i] %= p36;  /* Bring into Z_p36 */
    
    /* Setup L3-2 hash variables    */
    /* Fill buffer with index 4 key */
    kdf(ahc->ip_trans, prf_key, 4, STREAMS * sizeof(UINT32));
    endian_convert_if_le(ahc->ip_trans, sizeof(UINT32),
                         STREAMS * sizeof(UINT32));
}

/* ---------------------------------------------------------------------- */

uhash_ctx_t uhash_alloc(char key[])
{
/* Allocate memory and force to a 16-byte boundary. */
    uhash_ctx_t ctx;
    char bytes_to_add;
    aes_int_key prf_key;
    
    ctx = (uhash_ctx_t)malloc(sizeof(uhash_ctx)+ALLOC_BOUNDARY);
    if (ctx) {
        if (ALLOC_BOUNDARY) {
            bytes_to_add = ALLOC_BOUNDARY -
                              ((ptrdiff_t)ctx & (ALLOC_BOUNDARY -1));
            ctx = (uhash_ctx_t)((char *)ctx + bytes_to_add);
            *((char *)ctx - 1) = bytes_to_add;
        }
        aes_key_setup(key,prf_key);
        uhash_init(ctx, prf_key);
    }
    return (ctx);
}

/* ---------------------------------------------------------------------- */

int uhash_free(uhash_ctx_t ctx)
{
/* Free memory allocated by uhash_alloc */
    char bytes_to_sub;
    
    if (ctx) {
        if (ALLOC_BOUNDARY) {
            bytes_to_sub = *((char *)ctx - 1);
            ctx = (uhash_ctx_t)((char *)ctx - bytes_to_sub);
        }
        free(ctx);
    }
    return (1);
}

/* ---------------------------------------------------------------------- */

int uhash_update(uhash_ctx_t ctx, char *input, long len)
/* Given len bytes of data, we parse it into L1_KEY_LEN chunks and
 * hash each one with NH, calling the polyhash on each NH output.
 */
{
    UWORD bytes_hashed, bytes_remaining;
    UINT8 nh_result[STREAMS*sizeof(UINT64)];
    
    if (ctx->msg_len + len <= L1_KEY_LEN) {
        nh_update(&ctx->hash, (UINT8 *)input, len);
        ctx->msg_len += len;
    } else {
    
         bytes_hashed = ctx->msg_len % L1_KEY_LEN;
         if (ctx->msg_len == L1_KEY_LEN)
             bytes_hashed = L1_KEY_LEN;

         if (bytes_hashed + len >= L1_KEY_LEN) {

             /* If some bytes have been passed to the hash function      */
             /* then we want to pass at most (L1_KEY_LEN - bytes_hashed) */
             /* bytes to complete the current nh_block.                  */
             if (bytes_hashed) {
                 bytes_remaining = (L1_KEY_LEN - bytes_hashed);
                 nh_update(&ctx->hash, (UINT8 *)input, bytes_remaining);
                 nh_final(&ctx->hash, nh_result);
                 ctx->msg_len += bytes_remaining;
                 poly_hash(ctx,(UINT32 *)nh_result);
                 len -= bytes_remaining;
                 input += bytes_remaining;
             }

             /* Hash directly from input stream if enough bytes */
             while (len >= L1_KEY_LEN) {
                 nh(&ctx->hash, (UINT8 *)input, L1_KEY_LEN,
                                   L1_KEY_LEN, nh_result);
                 ctx->msg_len += L1_KEY_LEN;
                 len -= L1_KEY_LEN;
                 input += L1_KEY_LEN;
                 poly_hash(ctx,(UINT32 *)nh_result);
             }
         }

         /* pass remaining < L1_KEY_LEN bytes of input data to NH */
         if (len) {
             nh_update(&ctx->hash, (UINT8 *)input, len);
             ctx->msg_len += len;
         }
     }

    return (1);
}

/* ---------------------------------------------------------------------- */

int uhash_final(uhash_ctx_t ctx, char *res)
/* Incorporate any pending data, pad, and generate tag */
{
    UINT8 nh_result[STREAMS*sizeof(UINT64)];

    if (ctx->msg_len > L1_KEY_LEN) {
        if (ctx->msg_len % L1_KEY_LEN) {
            nh_final(&ctx->hash, nh_result);
            poly_hash(ctx,(UINT32 *)nh_result);
        }
        ip_long(ctx, res);
    } else {
        nh_final(&ctx->hash, nh_result);
        ip_short(ctx,nh_result, res);
    }
    uhash_reset(ctx);
    return (1);
}

/* ---------------------------------------------------------------------- */

int uhash(uhash_ctx_t ahc, char *msg, long len, char *res)
/* assumes that msg is in a writable buffer of length divisible by */
/* L1_PAD_BOUNDARY. Bytes beyond msg[len] may be zeroed.           */
{
    UINT8 nh_result[STREAMS*sizeof(UINT64)];
    UINT32 nh_len;
    int extra_zeroes_needed;
        
    /* If the message to be hashed is no longer than L1_HASH_LEN, we skip
     * the polyhash.
     */
    if (len <= L1_KEY_LEN) {
    	if (len == 0)                  /* If zero length messages will not */
    		nh_len = L1_PAD_BOUNDARY;  /* be seen, comment out this case   */ 
    	else
        	nh_len = ((len + (L1_PAD_BOUNDARY - 1)) & ~(L1_PAD_BOUNDARY - 1));
        if ( (extra_zeroes_needed = nh_len - len) ) {
	  zero_pad((UINT8 *)msg + len, extra_zeroes_needed);
	}
        nh(&ahc->hash, (UINT8 *)msg, nh_len, len, nh_result);
        ip_short(ahc,nh_result, res);
    } else {
        /* Otherwise, we hash each L1_KEY_LEN chunk with NH, passing the NH
         * output to poly_hash().
         */
        do {
            nh(&ahc->hash, (UINT8 *)msg, L1_KEY_LEN, L1_KEY_LEN, nh_result);
            poly_hash(ahc,(UINT32 *)nh_result);
            len -= L1_KEY_LEN;
            msg += L1_KEY_LEN;
        } while (len >= L1_KEY_LEN);
        if (len) {
            nh_len = ((len + (L1_PAD_BOUNDARY - 1)) & ~(L1_PAD_BOUNDARY - 1));
            extra_zeroes_needed = nh_len - len;
            zero_pad((UINT8 *)msg + len, extra_zeroes_needed);
            nh(&ahc->hash, (UINT8 *)msg, nh_len, len, nh_result);
            poly_hash(ahc,(UINT32 *)nh_result);
        }

        ip_long(ahc, res);
    }
    
    uhash_reset(ahc);
    return 1;
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- Begin UMAC Section --------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* The UMAC interface has two interfaces, an all-at-once interface where
 * the entire message to be authenticated is passed to UMAC in one buffer,
 * and a sequential interface where the message is presented a little at a   
 * time. The all-at-once is more optimaized than the sequential version and
 * should be preferred when the sequential interface is not required. 
 */
typedef struct umac_ctx {
    uhash_ctx hash;          /* Hash function for message compression    */
    pdf_ctx pdf;             /* PDF for hashed output                    */
} umac_ctx;

/* ---------------------------------------------------------------------- */

int umac_reset(umac_ctx_t ctx)
/* Reset the hash function to begin a new authentication.        */
{
    uhash_reset(&ctx->hash);
    return (1);
}

/* ---------------------------------------------------------------------- */

int umac_delete(umac_ctx_t ctx)
/* Deallocate the ctx structure */
{
    char bytes_to_sub;
    
    if (ctx) {
        if (ALLOC_BOUNDARY) {
            bytes_to_sub = *((char *)ctx - 1);
            ctx = (umac_ctx_t)((char *)ctx - bytes_to_sub);
        }
        free(ctx);
    }
    return (1);
}

/* ---------------------------------------------------------------------- */

umac_ctx_t umac_new(char key[])
/* Dynamically allocate a umac_ctx struct, initialize variables, 
 * generate subkeys from key. Align to 16-byte boundary.
 */
{
    umac_ctx_t ctx;
    char bytes_to_add;
    aes_int_key prf_key;
    
    ctx = (umac_ctx_t)malloc(sizeof(umac_ctx)+ALLOC_BOUNDARY);
    if (ctx) {
        if (ALLOC_BOUNDARY) {
            bytes_to_add = ALLOC_BOUNDARY -
                              ((ptrdiff_t)ctx & (ALLOC_BOUNDARY - 1));
            ctx = (umac_ctx_t)((char *)ctx + bytes_to_add);
            *((char *)ctx - 1) = bytes_to_add;
        }
        aes_key_setup(key,prf_key);
        pdf_init(&ctx->pdf, prf_key);
        uhash_init(&ctx->hash, prf_key);
    }
        
    return (ctx);
}

/* ---------------------------------------------------------------------- */

int umac_final(umac_ctx_t ctx, char tag[], char nonce[8])
/* Incorporate any pending data, pad, and generate tag */
{
    uhash_final(&ctx->hash, (char *)tag);
    pdf_gen_xor(&ctx->pdf, (UINT8 *)nonce, (UINT8 *)tag);
    
    return (1);
}

/* ---------------------------------------------------------------------- */

int umac_update(umac_ctx_t ctx, char *input, long len)
/* Given len bytes of data, we parse it into L1_KEY_LEN chunks and   */
/* hash each one, calling the PDF on the hashed output whenever the hash- */
/* output buffer is full.                                                 */
{
    uhash_update(&ctx->hash, input, len);
    return (1);
}

/* ---------------------------------------------------------------------- */

int umac(umac_ctx_t ctx, char *input, 
         long len, char tag[],
         char nonce[8])
/* All-in-one version simply calls umac_update() and umac_final().        */
{
    uhash(&ctx->hash, input, len, (char *)tag);
    pdf_gen_xor(&ctx->pdf, (UINT8 *)nonce, (UINT8 *)tag);
    
    return (1);
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ----- End UMAC Section ----------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* If RUN_TESTS is defined non-zero, then we define a main() function and */
/* run some verification and speed tests.                                 */

#if RUN_TESTS

#include <stdio.h>
#include <time.h>

static void pbuf(void *buf, UWORD n, char *s)
{
    UWORD i;
    UINT8 *cp = (UINT8 *)buf;
    
    if (n <= 0 || n >= 30)
        n = 30;
    
    if (s)
        printf("%s: ", s);
        
    for (i = 0; i < n; i++)
        printf("%02X", (unsigned char)cp[i]);
    printf("\n");
}

static void primitive_verify(void)
{
    #if (UMAC_KEY_LEN == 16)
    UINT8 key[16] = {0};
    UINT8 pt[16] = {'\x80',0,/* remainder auto filled with zeroes */};
    char res[] = "3AD78E726C1EC02B7EBFE92B23D9EC34";
    #elif (UMAC_KEY_LEN == 32)
    UINT8 key[32] = {0};
    UINT8 pt[16] = {'\x80',0,/* remainder auto filled with zeroes */};
    char res[] = "DDC6BF79 C1576 D8D9AEB6F9A75FD4E";
    #endif
    aes_int_key k1;
    
    aes_key_setup(key, k1);
    aes_encryption(pt, pt, k1);
    printf("\nAES Test\n");
    pbuf(pt, 16, "Digest is       ");
    printf("Digest should be: %s\n", res);
}

static int umac_verify(void)
{
    umac_ctx_t ctx;
    char *data_ptr;
    int data_len = 32 * 1024;
    char nonce[] = "abcdefgh";
    char tag[21] = {0};
    char tag2[21] = {0};
    int bytes_over_boundary, i, j;
    int inc[] = {1,99,512};
    int lengths[] = {0,3,1024,32768};
    char *results[] = {"4D61E4F5AAB959C8B800A2BE546302AD",
                       "67C1700CA30B532DCD9B970655B47B45",
                       "05CB9405EC38D9F0B356D9E6D5BC5D03", 
                       "048C543CB72443A46011A76438BA2AF4"};
    int return_value = 0;

    /* Initialize Memory and UMAC */
    data_ptr = (char *)malloc(data_len + 48);
    if (data_ptr == 0)
    	return;
    bytes_over_boundary = (ptrdiff_t)data_ptr & (16 - 1);
    if (bytes_over_boundary != 0)
        data_ptr += (16 - bytes_over_boundary);
    memset(data_ptr, 'a', data_len);
    ctx = umac_new("abcdefghijklmnop");
    
    printf("Testing known vectors.\n\n");
    printf("Msg           %-*s Is\n", UMAC_OUTPUT_LEN * 2, "Should be");
    printf("---           %-*s --\n", UMAC_OUTPUT_LEN * 2, "---------");
    
    for (i = 0; (unsigned)i < sizeof(lengths)/sizeof(*lengths); i++) {
    	memset(data_ptr, 'a', lengths[i]);
    	umac(ctx, data_ptr, lengths[i], tag, nonce);
    	umac_reset(ctx);    
    	printf("'a' * %5d : %.*s ", lengths[i], UMAC_OUTPUT_LEN * 2, results[i]);
    	pbuf(tag, UMAC_OUTPUT_LEN, NULL);
    }

    printf("\nVerifying consistancy of single- and"
           " multiple-call interfaces.\n");
    for (i = 1; i < (int)(sizeof(inc)/sizeof(inc[0])); i++) {
            for (j = 0; j <= data_len-inc[i]; j+=inc[i])
                umac_update(ctx, data_ptr+j, inc[i]);
            umac_final(ctx, tag, nonce);
            umac_reset(ctx);

            umac(ctx, data_ptr, (data_len/inc[i])*inc[i], tag2, nonce);
            umac_reset(ctx);
            nonce[7]++;
            
            if (memcmp(tag,tag2,sizeof(tag))) {
                printf("\ninc = %d data_len = %d failed!\n",
                       inc[i], data_len);
		return_value = 1;
	    }
    }
    printf("Done.\n");
    umac_delete(ctx);

    return return_value;
}


static double run_cpb_test(umac_ctx_t ctx, int nbytes, char *data_ptr,
                           int data_len, double hz)
{
    clock_t ticks;
    double secs;
    char nonce[8] = {0};
    char tag[UMAC_OUTPUT_LEN+1] = {0}; /* extra char for null terminator */
    unsigned long total_mbs;
    unsigned long iters_per_tag, remaining;
    unsigned long tag_iters, i, j;
    
    if (nbytes <= 16)
        total_mbs = 5;
    if (nbytes <= 32)
        total_mbs = 30;
    else if (nbytes <= 64)
        total_mbs = 400;
    else if (nbytes <= 256)
        total_mbs = 800;
    else if (nbytes <= 1024)
        total_mbs = 1600;
    else
        total_mbs = 2500;
    
    tag_iters = (total_mbs * 1024 * 1024) / (nbytes) + 1;
    
    if (nbytes <= data_len) {
    
        i = tag_iters;
        umac(ctx, data_ptr, nbytes, tag, nonce);
        ticks = clock();
        do {
            umac(ctx, data_ptr, nbytes, tag, nonce);
            nonce[7] += 1;
        } while (--i);
        ticks = clock() - ticks;
        
    } else {
    
        i = tag_iters;
        iters_per_tag = nbytes / data_len;
        remaining = nbytes % data_len;
        umac_update(ctx, data_ptr, data_len);
        umac_final(ctx, tag, nonce);
        ticks = clock();
        do {
            j = iters_per_tag;
            do {
                umac_update(ctx, data_ptr, data_len);
            } while (--j);
            if (remaining)
                umac_update(ctx, data_ptr, remaining);
            umac_final(ctx, tag, nonce);
            nonce[7] += 1;
        } while (--i);
        ticks = clock() - ticks;
        
    }

    secs = (double)ticks / CLOCKS_PER_SEC;
    return (secs * (hz/(tag_iters*nbytes)));
}

static void speed_test(void)
{
    umac_ctx_t ctx;
    char *data_ptr;
    int data_len;
    double hz;
    double cpb;
    int bytes_over_boundary, i;
    int length_range_low = 1;
    int length_range_high = 0;
    int length_pts[] = {44,64,256,512,552,1024,1500,8*1024,256*1024};
    
    /* hz and data_len must be set appropriately for your system
     * for optimal results.
     */
    #if  (GCC_X86 || MSC_X86)
    hz = ((double)2000e6);
    data_len = 4096;
    #else
    hz = ((double)1420e6);
    data_len = 8192;
    #endif

    /* Allocate memory and align to 16-byte multiple */
    data_ptr = (char *)malloc(data_len + 16);
    bytes_over_boundary = (ptrdiff_t)data_ptr & (16 - 1);
    if (bytes_over_boundary != 0)
        data_ptr += (16 - bytes_over_boundary);
    for (i = 0; i < data_len; i++)
        data_ptr[i] = (i*i) % 128;
    ctx = umac_new("abcdefghijklmnopqrstuvwxyz");
        
    printf("\n");
    if (length_range_low < length_range_high) {
        for (i = length_range_low; i <= length_range_high; i++) {
            cpb = run_cpb_test(ctx, i, data_ptr, data_len, hz);
            printf("Authenticating %8d byte messages: %5.2f cpb.\n", i, cpb);
        }
    }

    if (sizeof(length_pts) > 0) {
        for (i = 0; i < (int)(sizeof(length_pts)/sizeof(int)); i++) {
            cpb = run_cpb_test(ctx, length_pts[i], data_ptr, data_len, hz);
            printf("Authenticating %8d byte messages: %5.2f cpb.\n",
                                   length_pts[i], cpb);
        }
    }
    umac_delete(ctx);
}

int main(void)
{
    #if GLADMAN_AES
    gen_tabs();
    #endif
    if ( umac_verify() )
        return 1;
    primitive_verify();
    speed_test();
    /* printf("Push return to continue\n"); getchar(); */
    return (0);
}

#endif
