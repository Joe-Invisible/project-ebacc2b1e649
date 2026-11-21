#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
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

inline uint64_t hash_key(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    return key ^ (key >> 33);
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
    if (std::popcount(static_cast<uint64_t>(commas)) +
        std::popcount(static_cast<uint64_t>(commas >> 64)) != Width - 1) return false;

    size_t start = 0;
    CsvMask allowed_quotes = 0;
    for (size_t column = 0; column < Width; ++column) {
        size_t end = line.size();
        if (column + 1 < Width) {
            auto low = static_cast<uint64_t>(commas);
            end = low ? std::countr_zero(low) : 64 + std::countr_zero(static_cast<uint64_t>(commas >> 64));
            commas &= commas - 1;
        }
        auto value = line.substr(start, end - start);
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            allowed_quotes |= (CsvMask{1} << start) | (CsvMask{1} << (end - 1));
            value.remove_prefix(1);
            value.remove_suffix(1);
        }
        fields[column] = value;
        start = end + 1;
    }
    return quotes == allowed_quotes && !fields[0].empty();
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
        size_t comma = line.find(',');
        if ((comma == std::string_view::npos) != (column + 1 == Width)) return false;
        auto value = line.substr(0, comma);
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value.remove_prefix(1);
            value.remove_suffix(1);
        }
        if (value.find('"') != std::string_view::npos) return false;
        fields[column] = value;
        if (column + 1 < Width) line.remove_prefix(comma + 1);
    }
    return !fields[0].empty();
}

struct Part {
    const char* first;
    const char* end;
};

inline const char* next_line(const char* first, const char* end) {
    auto line = static_cast<const char*>(std::memchr(first, '\n', end - first));
    return line ? line + 1 : end;
}

class MappedInput {
    std::vector<Part> mappings;

public:
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
        first = next_line(first, end);
        while (first < end) {
            auto target = first + std::min<size_t>(chunk_bytes, end - first);
            auto last = target == end ? end : next_line(target, end);
            parts.push_back({first, last});
            first = last;
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
    std::array<std::string_view, Width> fields;
#ifdef __AVX2__
    if constexpr (Width >= 4) {
        const auto comma = _mm256_set1_epi8(',');
        const auto quote = _mm256_set1_epi8('"');
        const auto newline = _mm256_set1_epi8('\n');
        CsvMask commas = 0, quotes = 0;
        auto cursor = part.first;
        for (; part.end - cursor >= 32; cursor += 32) {
            auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(cursor));
            auto lines = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, newline)));
            auto block_commas = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, comma)));
            auto block_quotes = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote)));
            size_t offset = 0;
            auto append = [&](size_t end) {
                const auto count = end - offset;
                const auto carried = cursor + offset - part.first;
                if (count && carried + count <= 128) {
                    const auto keep = (uint64_t{1} << count) - 1;
                    commas |= CsvMask{(block_commas >> offset) & keep} << carried;
                    quotes |= CsvMask{(block_quotes >> offset) & keep} << carried;
                }
            };
            while (lines) {
                auto end = static_cast<size_t>(std::countr_zero(lines));
                append(end);
                auto line = std::string_view(part.first, cursor + end - part.first);
                bool valid = line.size() <= 128 ? split_masks(line, commas, quotes, fields) : split<Width>(line, fields);
                if (valid) visit(fields);
                lines &= lines - 1;
                offset = end + 1;
                part.first = cursor + offset;
                commas = quotes = 0;
            }
            append(32);
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

template <class Function>
void parallel(size_t workers, size_t count, Function&& work) {
    std::atomic<size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::exception_ptr error;
    auto run = [&](size_t worker) {
        try {
            for (size_t task; !failed && (task = next.fetch_add(1, std::memory_order_relaxed)) < count;) {
                work(worker, task);
            }
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
    if (error) std::rethrow_exception(error);
}

template <class T>
class Lookup {
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

    void insert(Key key, T value) {
        size_t slot = hash_key(key) & (slots.size() - 1);
        while (slots[slot].key && slots[slot].key != key) slot = (slot + 1) & (slots.size() - 1);
        slots[slot] = {key, value};
    }

    // The graph supplies each key once. Readers begin after all writers join.
    void insert_distinct_concurrently(Key key, T value) {
        size_t slot = hash_key(key) & (slots.size() - 1);
        for (;;) {
            Key empty = 0;
            if (std::atomic_ref<Key>(slots[slot].key).compare_exchange_strong(
                    empty, key, std::memory_order_relaxed)) {
                slots[slot].value = value;
                return;
            }
            slot = (slot + 1) & (slots.size() - 1);
        }
    }

    T find_or_insert(Key key, T value) {
        size_t slot = hash_key(key) & (slots.size() - 1);
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

    void index_homes() {
        occupied_homes.assign((slots.size() + 63) / 64, 0);
        for_each([&](Key key, T) {
            size_t home = hash_key(key) & (slots.size() - 1);
            occupied_homes[home / 64] |= uint64_t{1} << (home % 64);
        });
    }

    T get_filtered(Key key, uint64_t hash, T missing) const {
        size_t home = hash & (slots.size() - 1);
        if (!(occupied_homes[home / 64] & (uint64_t{1} << (home % 64)))) return missing;
        return probe(key, home, missing);
    }

    T get(Key key, T missing) const {
        return probe(key, hash_key(key) & (slots.size() - 1), missing);
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
