// pstwriter/crc.hpp
//
// PST CRC-32 ([MS-PST] §5.3).
//
// IMPORTANT: this is NOT zlib's crc32().
//   * Initial value: 0x00000000   (zlib uses 0xFFFFFFFF)
//   * Final XOR    : none         (zlib XORs result with 0xFFFFFFFF)
//   * Update form  : reflected lookup table from §5.3
//
// Calling zlib's crc32() in place of this function will silently corrupt
// every PST you produce. Outlook will reject the file with a generic error.

#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

using namespace std;

namespace pstwriter {

// Compute PST CRC-32 over `[data, data + length)`.
// `data` may be null only if `length == 0`.
uint32_t crc32(const uint8_t* data, size_t length) noexcept;

// Convenience overload for the byte-vector buffers used throughout the writer.
inline uint32_t crc32(const vector<uint8_t>& bytes) noexcept
{
    return crc32(bytes.data(), bytes.size());
}

} // namespace pstwriter
