#pragma once

#include <algorithm>
#include <cerrno>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <type_traits>
#include <unordered_map>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__BMI2__) || defined(__AVX2__)
#include <immintrin.h>
#endif

namespace shopping {
namespace fs = std::filesystem;
using Key = uint64_t;

inline constexpr uint64_t key_multiplier = 0xff51afd7ed558ccdULL;
inline constexpr uint64_t inverse_key_multiplier = 0x4f74430c22a54005ULL;
static_assert(key_multiplier * inverse_key_multiplier == 1);

// The xor-shift is its own inverse, and the odd multiplier is invertible.
inline uint64_t hash_key(uint64_t key) {
    key ^= key >> 33;
    key *= key_multiplier;
    return key ^ (key >> 33);
}

// The mixer permutes all 64-bit keys, so a stored hash preserves exact identity.
inline Key unhash_key(uint64_t hash) {
    hash ^= hash >> 33;
    hash *= inverse_key_multiplier;
    return hash ^ (hash >> 33);
}

// Thrown when input exceeds a fixed native layout; the caller then uses the
// reference pipeline instead.
struct InputLayoutLimit {};

// Short ASCII keys preserve byte order. Other strings are interned exactly in
// a lock-free table that every parsing thread reads and inserts into at once;
// the slot number becomes the key, and each string is stored once.
class Keys {
    static constexpr Key external = Key{1} << 63;
    static constexpr uint64_t busy = ~uint64_t{0};
    // A long ID of one byte plus 32 lowercase hex digits (the generator's
    // shape) is stored inline as that byte and 16 packed bytes. Any other
    // long ID points to a length-prefixed copy in a storage chunk.
    struct alignas(32) Slot {
        std::atomic<uint64_t> tag{0};
        uint64_t head = 0; // hex_form | first byte, or a pointer to the copy.
        uint64_t high = 0, low = 0;
    };
    static constexpr uint64_t hex_form = uint64_t{1} << 63;
    struct Packed {
        uint64_t head, high, low;
        bool hex;
    };
    Slot* slots = nullptr;
    size_t mask = 0;
    // Bounds probing instead of counting entries, which would make every
    // thread write one shared cache line. Only a nearly full table reaches it.
    static constexpr size_t max_probes = 4096;

    static Key short_key(std::string_view value) {
        if (value.empty() || value.size() > 9) return 0;
#ifdef __BMI2__
        constexpr uint64_t low = 0x0101010101010101ULL;
        constexpr uint64_t high = 0x8080808080808080ULL;
        uint64_t bytes = 0;
        const auto width = std::min<size_t>(8, value.size());
        std::memcpy(&bytes, value.data(), width);
        const uint64_t filled = width == 8 ? bytes : bytes | (~uint64_t{0} << (width * 8));
        const auto last = value.size() == 9 ? static_cast<unsigned char>(value[8]) : 0;
        if ((bytes & high) || ((filled - low) & ~filled & high) ||
            (value.size() == 9 && (last == 0 || last > 127))) return 0;
        return (_pext_u64(__builtin_bswap64(bytes), ~high) << 7) | last;
#else
        Key result = 0;
        for (size_t i = 0; i < value.size(); ++i) {
            auto byte = static_cast<unsigned char>(value[i]);
            if (byte == 0 || byte > 127) return 0;
            result |= Key{byte} << (56 - i * 7);
        }
        return result;
#endif
    }

    static uint64_t hash_text(std::string_view value) {
        uint64_t hash = 0x9e3779b97f4a7c15ULL ^ value.size();
        size_t i = 0;
        for (; i + 8 <= value.size(); i += 8) {
            uint64_t word;
            std::memcpy(&word, value.data() + i, 8);
            hash = (hash ^ word) * 0xbf58476d1ce4e5b9ULL;
            hash ^= hash >> 31;
        }
        if (i < value.size()) {
            uint64_t word = 0;
            std::memcpy(&word, value.data() + i, value.size() - i);
            hash = (hash ^ word) * 0xbf58476d1ce4e5b9ULL;
            hash ^= hash >> 31;
        }
        hash ^= hash >> 29;
        hash *= 0x94d049bb133111ebULL;
        return hash ^ (hash >> 32);
    }

    // Copies a string into this thread's storage. Chunks live until exit.
    static const char* store(std::string_view value) {
        struct Chunk {
            char* next = nullptr;
            char* end = nullptr;
        };
        thread_local Chunk chunk;
        const size_t bytes = (sizeof(uint32_t) + value.size() + 7) & ~size_t{7};
        if (static_cast<size_t>(chunk.end - chunk.next) < bytes) {
            const size_t size = std::max<size_t>(bytes, 1 << 20);
            chunk.next = static_cast<char*>(std::malloc(size));
            if (!chunk.next) throw std::bad_alloc{};
            chunk.end = chunk.next + size;
        }
        char* text = chunk.next;
        chunk.next += bytes;
        const auto length = static_cast<uint32_t>(value.size());
        std::memcpy(text, &length, sizeof length);
        std::memcpy(text + sizeof length, value.data(), value.size());
        return text;
    }

    // Packs "<byte><32 lowercase hex digits>" exactly, or reports another shape.
    static bool pack_hex(std::string_view value, Packed& packed) {
        if (value.size() != 33) return false;
        const char* digits = value.data() + 1;
#ifdef __AVX2__
        const auto text = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(digits));
        // Digits '0'-'9' and letters 'a'-'f' only, each mapped to its value.
        const auto is_digit = _mm256_and_si256(_mm256_cmpgt_epi8(text, _mm256_set1_epi8('0' - 1)),
                                               _mm256_cmpgt_epi8(_mm256_set1_epi8('9' + 1), text));
        const auto is_letter = _mm256_and_si256(_mm256_cmpgt_epi8(text, _mm256_set1_epi8('a' - 1)),
                                                _mm256_cmpgt_epi8(_mm256_set1_epi8('f' + 1), text));
        if (static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_or_si256(is_digit, is_letter))) != UINT32_MAX) {
            return false;
        }
        const auto values = _mm256_add_epi8(_mm256_and_si256(text, _mm256_set1_epi8(0x0f)),
                                            _mm256_and_si256(is_letter, _mm256_set1_epi8(9)));
        // Each pair of digits becomes one byte: first * 16 + second.
        const auto pairs = _mm256_maddubs_epi16(values, _mm256_set1_epi16(0x0110));
        const auto bytes = _mm256_packus_epi16(pairs, pairs); // lanes: [0..7 x], [8..15 x]
        alignas(32) uint8_t out[32];
        _mm256_store_si256(reinterpret_cast<__m256i*>(out), bytes);
        uint64_t high, low;
        std::memcpy(&high, out, 8);
        std::memcpy(&low, out + 16, 8);
        packed = {hex_form | static_cast<unsigned char>(value[0]), __builtin_bswap64(high), __builtin_bswap64(low), true};
        return true;
#else
        uint64_t high = 0, low = 0;
        for (int i = 0; i < 32; ++i) {
            const auto c = static_cast<unsigned char>(digits[i]);
            uint64_t nibble;
            if (c >= '0' && c <= '9') nibble = c - '0';
            else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10;
            else return false;
            (i < 16 ? high : low) = ((i < 16 ? high : low) << 4) | nibble;
        }
        packed = {hex_form | static_cast<unsigned char>(value[0]), high, low, true};
        return true;
#endif
    }

    static uint64_t hash_packed(const Packed& packed) {
        uint64_t hash = (packed.head ^ 0x9e3779b97f4a7c15ULL) * 0xbf58476d1ce4e5b9ULL;
        hash = (hash ^ packed.high ^ (hash >> 31)) * 0x94d049bb133111ebULL;
        hash = (hash ^ packed.low ^ (hash >> 29)) * 0xbf58476d1ce4e5b9ULL;
        return hash ^ (hash >> 32);
    }

    static std::string_view unpack_hex(const Slot& slot, std::array<char, 40>& buffer) {
        constexpr char digits[] = "0123456789abcdef";
        buffer[0] = static_cast<char>(slot.head & 0xff);
        for (int i = 0; i < 16; ++i) buffer[1 + i] = digits[(slot.high >> (60 - 4 * i)) & 15];
        for (int i = 0; i < 16; ++i) buffer[17 + i] = digits[(slot.low >> (60 - 4 * i)) & 15];
        return {buffer.data(), 33};
    }

    static std::string_view text_of(const char* text) {
        uint32_t length;
        std::memcpy(&length, text, sizeof length);
        return {text + sizeof length, length};
    }

public:
    // Anonymous pages read as zero, which is an empty slot, so the table
    // needs no initialization pass. It lives until the process exits.
    explicit Keys(size_t capacity = 1 << 16) {
        static_assert(std::is_trivially_destructible_v<Slot>);
        const size_t size = std::bit_ceil(std::max<size_t>(capacity, 1024));
        auto memory = mmap(nullptr, size * sizeof(Slot), PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (memory == MAP_FAILED) throw std::bad_alloc{};
        const char* huge = std::getenv("SHOPPING_HUGEPAGES");
        if (!huge || std::string_view(huge) != "0") madvise(memory, size * sizeof(Slot), MADV_HUGEPAGE);
        slots = static_cast<Slot*>(memory);
        mask = size - 1;
    }
    Keys(const Keys&) = delete;

    // Rebuilds an ID packed by pack_long_id from its first byte and digits.
    static std::string_view unpack_long_id(unsigned char first, uint64_t high, uint64_t low,
                                           std::array<char, 40>& buffer) {
        Slot slot;
        slot.head = hex_form | first;
        slot.high = high;
        slot.low = low;
        return unpack_hex(slot, buffer);
    }

    // For an ID of one byte plus 32 lowercase hex digits, gives its exact
    // packed form without using the table: a hash in the high 56 bits of id
    // with the first byte in the low 8, and the digits in high and low.
    static bool pack_long_id(std::string_view value, uint64_t& id, uint64_t& high, uint64_t& low) {
        Packed packed{};
        if (!pack_hex(value, packed)) return false;
        id = (hash_packed(packed) & ~uint64_t{0xff}) | (packed.head & 0xff);
        high = packed.high;
        low = packed.low;
        return true;
    }

    // An ID hashed ahead of its lookup, with the table slot prefetched, so
    // the cache miss overlaps other work on the row.
    struct Prepared {
        std::string_view value;
        Key key; // Final key for empty and short IDs.
        uint64_t tag;
        Packed packed;
    };

    Prepared prepare(std::string_view value) const {
        Prepared prepared{value, 0, 0, {}};
        if (value.empty()) return prepared;
        if ((prepared.key = short_key(value))) return prepared;
        prepared.tag = pack_hex(value, prepared.packed) ? hash_packed(prepared.packed) : hash_text(value);
        if (prepared.tag == 0 || prepared.tag == busy) prepared.tag = 1;
        __builtin_prefetch(&slots[prepared.tag & mask], 1, 3);
        return prepared;
    }

    Key encode(std::string_view value) { return resolve(prepare(value)); }

    Key resolve(const Prepared& prepared) {
        if (prepared.key || prepared.value.empty()) return prepared.key;
        const auto value = prepared.value;
        if (value.size() > UINT32_MAX) throw std::length_error("ID too long");
        const auto& packed = prepared.packed;
        const auto tag = prepared.tag;
        size_t slot = tag & mask;
        for (size_t probes = 0; probes < max_probes; ++probes, slot = (slot + 1) & mask) {
            auto& entry = slots[slot];
            auto current = entry.tag.load(std::memory_order_acquire);
            if (current == 0 &&
                entry.tag.compare_exchange_strong(current, busy, std::memory_order_acquire)) {
                if (packed.hex) {
                    entry.head = packed.head;
                    entry.high = packed.high;
                    entry.low = packed.low;
                } else {
                    entry.head = reinterpret_cast<uint64_t>(store(value));
                }
                entry.tag.store(tag, std::memory_order_release);
                return external | slot;
            }
            // Another thread is storing this slot's string. Wait for it.
            while (current == busy) {
#if defined(__x86_64__)
                __builtin_ia32_pause();
#endif
                current = entry.tag.load(std::memory_order_acquire);
            }
            if (current != tag) continue;
            if (packed.hex) {
                if (entry.head == packed.head && entry.high == packed.high && entry.low == packed.low) {
                    return external | slot;
                }
            } else if (!(entry.head & hex_form) &&
                       text_of(reinterpret_cast<const char*>(entry.head)) == value) {
                return external | slot;
            }
        }
        throw InputLayoutLimit{};
    }

    // Large enough for a short key or an inline long ID.
    using Buffer = std::array<char, 40>;

    std::string_view decode(Key key, Buffer& buffer) const {
        if (key & external) {
            const auto& slot = slots[key & ~external];
            if (slot.head & hex_form) return unpack_hex(slot, buffer);
            return text_of(reinterpret_cast<const char*>(slot.head));
        }
        size_t size = 0;
        for (int shift = 56; shift >= 0; shift -= 7) {
            auto byte = static_cast<char>((key >> shift) & 127);
            if (!byte) break;
            buffer[size++] = byte;
        }
        return {buffer.data(), size};
    }

    bool less(Key a, Key b) const {
        if (!((a | b) & external)) return a < b;
        Buffer x, y;
        return decode(a, x).compare(decode(b, y)) < 0;
    }
};

#ifdef __AVX2__
using CsvMask = __uint128_t;

template <size_t Width>
bool split_masks(std::string_view line, CsvMask commas, CsvMask quotes,
                 std::array<std::string_view, Width>& fields) {
    auto low = static_cast<uint64_t>(commas);
    auto high = static_cast<uint64_t>(commas >> 64);
    if (std::popcount(low) + std::popcount(high) != Width - 1) return false;

    size_t start = 0;
    unsigned accepted_quotes = 0;
    for (size_t column = 0; column < Width; ++column) {
        size_t end = line.size();
        if (column + 1 < Width) {
            if (low) {
                end = std::countr_zero(low);
                low &= low - 1;
            } else {
                end = 64 + std::countr_zero(high);
                high &= high - 1;
            }
        }
        const auto length = end - start;
        // A matched pair accounts for two distinct quote bytes. Comparing the
        // count with the SIMD mask also rejects quotes anywhere inside a field.
        unsigned quoted = 0;
        if (length >= 2) quoted = (line[start] == '"') & (line[end - 1] == '"');
        accepted_quotes += 2 * quoted;
        fields[column] = {line.data() + start + quoted, length - 2 * quoted};
        start = end + 1;
    }
    if (!fields[0].size()) return false;
    return accepted_quotes == static_cast<unsigned>(std::popcount(static_cast<uint64_t>(quotes)) +
                                                     std::popcount(static_cast<uint64_t>(quotes >> 64)));
}

template <size_t Width>
bool split_vectorized(std::string_view line, std::array<std::string_view, Width>& fields) {
    CsvMask commas = 0, quotes = 0;
    const auto comma = _mm256_set1_epi8(',');
    const auto quote = _mm256_set1_epi8('"');
    auto scan = [&](size_t offset, size_t skip, size_t out) {
        auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(line.data() + offset));
        auto comma_bits = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, comma)));
        auto quote_bits = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote)));
        commas |= static_cast<CsvMask>(comma_bits >> skip) << out;
        quotes |= static_cast<CsvMask>(quote_bits >> skip) << out;
    };
    size_t scanned = 0;
    for (; line.size() - scanned >= 32; scanned += 32) scan(scanned, 0, scanned);
    if (scanned != line.size()) scan(line.size() - 32, scanned + 32 - line.size(), scanned);
    return split_masks(line, commas, quotes, fields);
}
#endif

template <size_t Width>
bool split(std::string_view line, std::array<std::string_view, Width>& fields) {
#ifdef __AVX2__
    if (line.size() >= 32 && line.size() <= 128) return split_vectorized(line, fields);
#endif
    for (size_t column = 0; column < Width; ++column) {
        const auto comma = line.find(',');
        auto value = line;
        if (column + 1 < Width) {
            if (comma >= line.size()) return false;
            value = {line.data(), comma};
            line.remove_prefix(comma + 1);
        } else if (comma != std::string_view::npos) {
            return false;
        }
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value.remove_prefix(1);
            value.remove_suffix(1);
        }
        if (value.find('"') != std::string_view::npos) return false;
        fields[column] = value;
    }
    return !fields[0].empty();
}

// Return the whole pages inside a buffer to the kernel. The buffer stays
// valid (it reads back as zeros), so it may still be freed or reused later.
// Unlike munmap, this takes the address-space lock shared, so workers can
// release memory concurrently.
inline void discard_memory(const void* data, size_t bytes) {
    constexpr uintptr_t page = 4096;
    const auto first = (reinterpret_cast<uintptr_t>(data) + page - 1) & ~(page - 1);
    const auto last = (reinterpret_cast<uintptr_t>(data) + bytes) & ~(page - 1);
    if (last > first) madvise(reinterpret_cast<void*>(first), last - first, MADV_DONTNEED);
}

struct Part {
    const char* first;
    const char* end;
    // A non-null file end means that the chunk still needs row alignment.
    const char* file_end = nullptr;
};

inline const char* next_line(const char* first, const char* end) {
    auto line = static_cast<const char*>(std::memchr(first, '\n', end - first));
    return line ? line + 1 : end;
}

inline Part align_part(Part part) {
    if (!part.file_end) return part;
    // The first chunk skips its header. Every other chunk starts after
    // the row owned by its predecessor, even when a row spans many chunks.
    return {next_line(part.first, part.file_end),
            part.end == part.file_end ? part.end : next_line(part.end, part.file_end)};
}

class MappedInput {
    std::vector<Part> mappings;

public:
    // Drop the mapped pages of every task-th chunk, starting at index.
    // Chunk boundaries are not page aligned, so each range is widened to
    // whole pages. Neighbouring chunks may both drop a shared page.
    void discard(size_t index, size_t tasks) const {
        constexpr uintptr_t page = 4096;
        for (size_t part = index; part < parts.size(); part += tasks) {
            const auto first = reinterpret_cast<uintptr_t>(parts[part].first) & ~(page - 1);
            const auto last = reinterpret_cast<uintptr_t>(parts[part].end);
            madvise(reinterpret_cast<void*>(first), last - first, MADV_DONTNEED);
        }
    }
    std::vector<Part> parts;
    MappedInput() = default;
    MappedInput(const MappedInput&) = delete;
    void add(const fs::path& path, size_t chunk_bytes) {
        int file = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (file < 0) throw std::runtime_error("cannot open " + path.string());
        struct stat info {};
        if (fstat(file, &info) != 0) {
            close(file);
            throw std::runtime_error("cannot stat " + path.string());
        }
        if (!info.st_size) {
            close(file);
            return;
        }
        auto address = mmap(nullptr, info.st_size, PROT_READ, MAP_PRIVATE, file, 0);
        close(file);
        if (address == MAP_FAILED) throw std::runtime_error("cannot map " + path.string());
        auto first = static_cast<const char*>(address);
        auto end = first + info.st_size;
        mappings.push_back({first, end});
        while (first < end) {
            auto target = first + std::min<size_t>(chunk_bytes, end - first);
            parts.push_back({first, target, end});
            first = target;
        }
    }
    void folder(const fs::path& path, size_t chunk_bytes) {
        if (!fs::exists(path)) return;
        for (auto& entry : fs::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().filename().native().ends_with(".csv")) {
                add(entry.path(), chunk_bytes);
            }
        }
    }
    ~MappedInput() {
        for (auto part : mappings) munmap(const_cast<char*>(part.first), part.end - part.first);
    }
};

// Reads CSV files in fixed-size chunks with pread into a caller-owned buffer
// that stays cache resident. Unlike a file mapping, this needs no page-table
// setup for the input and nothing to tear down afterwards.
class ChunkedInput {
    struct Chunk {
        int file;
        uint64_t offset, size, file_size;
    };
    std::vector<int> files;
    static constexpr size_t slack = 64 * 1024;

    static void read_exactly(int file, char* data, size_t size, uint64_t offset) {
        while (size) {
            const auto count = pread(file, data, size, static_cast<off_t>(offset));
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) throw std::runtime_error("cannot read input");
            data += count;
            size -= static_cast<size_t>(count);
            offset += static_cast<uint64_t>(count);
        }
    }

public:
    std::vector<Chunk> chunks;
    ChunkedInput() = default;
    ChunkedInput(const ChunkedInput&) = delete;
    ~ChunkedInput() {
        for (int file : files) close(file);
    }

    void add(const fs::path& path, size_t chunk_bytes) {
        int file = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (file < 0) throw std::runtime_error("cannot open " + path.string());
        struct stat info {};
        if (fstat(file, &info) != 0) {
            close(file);
            throw std::runtime_error("cannot stat " + path.string());
        }
        const auto size = static_cast<uint64_t>(info.st_size);
        if (!size) {
            close(file);
            return;
        }
        files.push_back(file);
        for (uint64_t offset = 0; offset < size; offset += chunk_bytes) {
            chunks.push_back({file, offset, std::min<uint64_t>(chunk_bytes, size - offset), size});
        }
    }

    void folder(const fs::path& path, size_t chunk_bytes) {
        if (!fs::exists(path)) return;
        for (auto& entry : fs::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().filename().native().ends_with(".csv")) {
                add(entry.path(), chunk_bytes);
            }
        }
    }

    size_t bytes() const {
        size_t total = 0;
        for (const auto& chunk : chunks) total += chunk.size;
        return total;
    }

    // Loads a chunk plus enough of the following bytes to finish its last
    // row. The returned part still needs row alignment, as a mapped one does.
    Part load(size_t index, std::vector<char>& buffer) const {
        const auto& chunk = chunks[index];
        const auto available = chunk.file_size - chunk.offset;
        size_t want = std::min<uint64_t>(chunk.size + slack, available);
        for (;;) {
            if (buffer.size() < want) buffer.resize(want);
            read_exactly(chunk.file, buffer.data(), want, chunk.offset);
            const char* data = buffer.data();
            const char* part_end = data + chunk.size;
            const char* file_end = data + want;
            if (want == available || std::memchr(part_end, '\n', file_end - part_end)) {
                return {data, part_end, file_end};
            }
            // The last row continues past the slack. Read further.
            want = std::min<uint64_t>(want * 2, available);
        }
    }

    // Estimates the row count from samples of evenly spaced chunks.
    size_t estimate_rows(size_t minimum_width) const {
        if (chunks.empty()) return 0;
        std::vector<char> buffer;
        size_t sampled_bytes = 0, sampled_rows = 0;
        const size_t interval = std::max<size_t>(1, chunks.size() / 16);
        for (size_t i = 0; i < chunks.size(); i += interval) {
            const auto& chunk = chunks[i];
            const size_t size = std::min<uint64_t>(16 * 1024, chunk.file_size - chunk.offset);
            buffer.resize(size);
            read_exactly(chunk.file, buffer.data(), size, chunk.offset);
            const char* first = buffer.data();
            const char* end = first + size;
            const char* line = static_cast<const char*>(std::memchr(first, '\n', size));
            if (!line) continue;
            const char* row = ++line;
            for (size_t n = 0; n < 128 && line < end; ++n) {
                auto next = static_cast<const char*>(std::memchr(line, '\n', end - line));
                if (!next) break;
                line = next + 1;
                ++sampled_rows;
            }
            sampled_bytes += line - row;
        }
        if (!sampled_rows) return 0;
        // Sampled density controls capacity only. Malformed short rows do not
        // inflate the reservation beyond the minimum possible row width.
        const auto average = std::max(static_cast<double>(minimum_width),
                                      static_cast<double>(sampled_bytes) / sampled_rows);
        return static_cast<size_t>(bytes() / average);
    }
};

template <size_t Width, class Function>
void read_part(Part part, Function&& visit) {
    part = align_part(part);
    std::array<std::string_view, Width> fields;
#ifdef __AVX2__
    if constexpr (Width >= 2) {
        const auto comma = _mm256_set1_epi8(',');
        const auto quote = _mm256_set1_epi8('"');
        const auto newline = _mm256_set1_epi8('\n');
        // Scan a short row from its first byte. Each aligned mask has a
        // constant shift, and bytes past the first newline are excluded.
        while (part.end - part.first >= 128) {
            CsvMask commas = 0, quotes = 0;
            const char* last = nullptr;
            auto scan = [&]<size_t Offset>() {
                const auto* cursor = part.first + Offset;
                auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(cursor));
                auto lines = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, newline)));
                uint32_t keep = UINT32_MAX;
                if (lines) {
                    const auto index = std::countr_zero(lines);
                    keep = static_cast<uint32_t>((uint64_t{1} << index) - 1);
                    last = cursor + index;
                }
                auto column = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, comma)));
                auto quoted = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote)));
                commas |= CsvMask{column & keep} << Offset;
                quotes |= CsvMask{quoted & keep} << Offset;
                return lines != 0;
            };
            if (scan.template operator()<0>() || scan.template operator()<32>() ||
                scan.template operator()<64>() || scan.template operator()<96>()) {
                if (split_masks({part.first, static_cast<size_t>(last - part.first)}, commas, quotes, fields)) visit(fields);
                part.first = last + 1;
            } else {
                auto next = next_line(part.first + 128, part.end);
                size_t size = next - part.first - (next[-1] == '\n');
                if (split<Width>({part.first, size}, fields)) visit(fields);
                part.first = next;
            }
        }
    }

#endif
    while (part.first < part.end) {
        auto next = next_line(part.first, part.end);
        size_t size = next - part.first - (next[-1] == '\n');
        if (split<Width>({part.first, size}, fields)) visit(fields);
        part.first = next;
    }
}

inline bool profile_workers = false;

template <class Function>
void parallel(size_t workers, size_t count, Function&& work) {
    const auto began = std::chrono::steady_clock::now();
    std::vector<double> finished(std::min(workers, count));
    std::atomic<size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::exception_ptr error;
    auto run = [&](size_t worker) {
        try {
            for (size_t task; !failed && (task = next.fetch_add(1, std::memory_order_relaxed)) < count;) {
                work(worker, task);
            }
            finished[worker] = std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
        } catch (...) {
            std::lock_guard lock(error_mutex);
            if (!error) error = std::current_exception();
            failed = true;
        }
    };
    std::vector<std::jthread> threads;
    for (size_t worker = 1; worker < std::min(workers, count); ++worker) threads.emplace_back(run, worker);
    run(0);
    threads.clear();
    if (profile_workers && finished.size() > 1) {
        auto [low, high] = std::minmax_element(finished.begin(), finished.end());
        fprintf(stderr, "  parallel(%zu tasks): first worker done %.3f s, last %.3f s\n", count, *low, *high);
    }
    if (error) std::rethrow_exception(error);
}

template <class T, bool Prehashed = false>
class Lookup {
    static uint64_t slot_hash(Key key) {
        if constexpr (Prehashed) return key;
        return hash_key(key);
    }

    struct Slot {
        Key key = 0;
        T value{};
    };
    std::vector<Slot> slots;
    std::vector<uint64_t> occupied_homes;

public:
    explicit Lookup(size_t size = 0) {
        size_t capacity = 2;
        while (capacity < size * 2) capacity *= 2;
        slots.resize(capacity);
    }

    size_t capacity() const noexcept { return slots.size(); }

    void insert(Key key, T value) {
        size_t slot = slot_hash(key) & (slots.size() - 1);
        while (slots[slot].key && slots[slot].key != key) slot = (slot + 1) & (slots.size() - 1);
        slots[slot] = {key, value};
    }

    T find_or_insert(Key key, T value) {
        size_t slot = slot_hash(key) & (slots.size() - 1);
        while (slots[slot].key && slots[slot].key != key) slot = (slot + 1) & (slots.size() - 1);
        if (!slots[slot].key) slots[slot] = {key, value};
        return slots[slot].value;
    }

    void reserve(size_t count) {
        if (count * 2 <= slots.size()) return;
        Lookup resized(count);
        for_each([&](Key key, T value) { resized.insert(key, value); });
        slots.swap(resized.slots);
    }

    template<class Visit>
    void for_each(Visit&& visit) const {
        for (const auto& slot : slots) {
            if (slot.key) visit(slot.key, slot.value);
        }
    }

    template<class Update>
    void index_updated_values(Update update) {
        occupied_homes.assign((slots.size() + 63) / 64, 0);
        for (auto& slot : slots) {
            if (!slot.key || !update(slot.value)) continue;
            const auto home = slot_hash(slot.key) & (slots.size() - 1);
            occupied_homes[home / 64] |= uint64_t{1} << (home % 64);
        }
    }

    void prefetch_filtered(uint64_t hash) const {
        size_t home = hash & (slots.size() - 1);
        if (occupied_homes[home / 64] & (uint64_t{1} << (home % 64))) {
            __builtin_prefetch(&slots[home], 0, 1);
        }
    }

    T get_filtered(Key key, uint64_t hash, T missing) const {
        size_t home = hash & (slots.size() - 1);
        if (!(occupied_homes[home / 64] & (uint64_t{1} << (home % 64)))) return missing;
        return probe(key, home, missing);
    }

    T get(Key key, T missing) const {
        return probe(key, slot_hash(key) & (slots.size() - 1), missing);
    }

private:
    T probe(Key key, size_t slot, T missing) const {
        while (slots[slot].key != key) {
            if (!slots[slot].key) return missing;
            slot = (slot + 1) & (slots.size() - 1);
        }
        return key ? slots[slot].value : missing;
    }
};

} // namespace shopping
