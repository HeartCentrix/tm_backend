// pstwriter/encoding.hpp
//
// Block encoding for PST data blocks ([MS-PST] §5.1, §5.2).
//
// Two algorithms are defined by the spec:
//
//   * NDB_CRYPT_PERMUTE (§5.1):
//       byte-wise substitution using the mpbbR table from the §5.1
//       768-byte mpbbCrypt array. Pstwriter is write-only, so it only
//       ever applies mpbbR (the encrypt direction). The corresponding
//       inverse table mpbbI lives at offset 512 of permuteTable() for
//       any caller that wants to decode (e.g. test_encoding).
//
//   * NDB_CRYPT_CYCLIC (§5.2):
//       parameterised cipher keyed by the lower 32 bits of the block's
//       BID. The cipher is symmetric — calling encodeCyclic() twice with
//       the same key returns the original buffer.
//
// IMPORTANT order of operations when writing a block:
//   1.  Encode the plaintext payload bytes in place.
//   2.  Compute the BLOCKTRAILER CRC over the *encoded* bytes.
// Computing the CRC over plaintext bytes is the most common rejection
// cause: Outlook silently refuses the file with no useful error message.

#pragma once

#include "types.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

using namespace std;

namespace pstwriter {

// ---- NDB_CRYPT_PERMUTE (§5.1) --------------------------------------------
// Encode `[data, data + length)` in place by replacing each byte b with
// mpbbR[b].  Use this when writing data blocks with bCryptMethod == 0x01.
void encodePermute(uint8_t* data, size_t length) noexcept;

inline void encodePermute(vector<uint8_t>& bytes) noexcept
{
    encodePermute(bytes.data(), bytes.size());
}

// ---- NDB_CRYPT_CYCLIC (§5.2) ---------------------------------------------
// Symmetric cipher.  `key` is the lower 32 bits of the block's BID:
//     (uint32_t)(bid.value & 0xFFFFFFFFu)
// Calling encodeCyclic() twice with the same key returns the original
// buffer (this is exploited by test_encoding for the round-trip test).
void encodeCyclic(uint8_t* data, size_t length, uint32_t key) noexcept;

inline void encodeCyclic(vector<uint8_t>& bytes, uint32_t key) noexcept
{
    encodeCyclic(bytes.data(), bytes.size(), key);
}

// ---- Dispatch ------------------------------------------------------------
// Encode `[data, data + length)` using the algorithm selected by `method`.
// `key` is ignored for CryptMethod::None and CryptMethod::Permute.
// Throws nothing.
void encodeBlock(uint8_t*    data,
                 size_t      length,
                 CryptMethod method,
                 uint32_t    key) noexcept;

inline void encodeBlock(vector<uint8_t>& bytes,
                        CryptMethod      method,
                        uint32_t         key) noexcept
{
    encodeBlock(bytes.data(), bytes.size(), method, key);
}

// ---- Direct table access (test-only) -------------------------------------
// Returns the full 768-byte mpbbCrypt array from [MS-PST] §5.1.  Layout:
//     mpbbR = permuteTable() + 0     §5.1 encrypt
//     mpbbS = permuteTable() + 256   §5.2 mix
//     mpbbI = permuteTable() + 512   §5.1 decrypt (= inverse of mpbbR)
const uint8_t* permuteTable() noexcept;

} // namespace pstwriter
