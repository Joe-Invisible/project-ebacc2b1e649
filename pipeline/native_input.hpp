#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

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

// Short ASCII keys preserve byte order. Other strings are interned exactly.
class Keys {
    static constexpr Key external = Key{1} << 63;
    mutable std::mutex mutex;
    std::deque<std::string> strings;
    std::unordered_map<std::string_view, Key> indexes;

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

public:
    Key encode(std::string_view value) {
        if (value.empty()) return 0;
        if (auto key = short_key(value)) return key;
        std::lock_guard lock(mutex);
        if (auto existing = indexes.find(value); existing != indexes.end()) return existing->second;
        Key key = external | strings.size();
        strings.emplace_back(value);
        indexes.emplace(strings.back(), key);
        return key;
    }

    std::string_view decode(Key key, std::array<char, 9>& buffer) const {
        if (key & external) return strings[key & ~external];
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
        std::lock_guard lock(mutex);
        std::array<char, 9> x, y;
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
    // Drop the mapped pages of every task-th file, starting at index.
    void discard(size_t index, size_t tasks) const {
        for (size_t file = index; file < mappings.size(); file += tasks) {
            discard_memory(mappings[file].first, mappings[file].end - mappings[file].first);
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
