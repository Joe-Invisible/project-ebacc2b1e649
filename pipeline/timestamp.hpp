#pragma once

#include <array>
#include <charconv>
#include <cstring>
#include <cstdint>
#include <optional>
#include <string_view>
#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace time_detail {
constexpr auto years = [] {
    std::array<int32_t, 10000> result{};
    for (int year = 1; year < 10000; ++year) {
        int previous = year - 1;
        int jan = 365 * previous + previous / 4 - previous / 100 + previous / 400 - 719162;
        bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        result[year] = jan * 2 + leap;
    }
    return result;
}();
constexpr std::array<int, 13> months{0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
constexpr std::array<int, 13> lengths{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

struct Clock {
    int year, month, day, hour, minute, second;
};

inline int pair(const char* p) {
    unsigned a = static_cast<unsigned char>(p[0]) - '0';
    unsigned b = static_cast<unsigned char>(p[1]) - '0';
    return a <= 9 && b <= 9 ? static_cast<int>(a * 10 + b) : -1;
}

template<bool Validate>
inline Clock decode_clock(const char* p) {
#ifdef __AVX2__
    auto input = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    auto digits = _mm_sub_epi8(input, _mm_set1_epi8('0'));
    if constexpr (Validate) {
        auto nine = _mm_set1_epi8(9);
        auto valid = _mm_cmpeq_epi8(_mm_max_epu8(digits, nine), nine);
        constexpr int digit_positions = 0xDB6F;
        if ((_mm_movemask_epi8(valid) & digit_positions) != digit_positions) return {};
    }
    auto packed = _mm_shuffle_epi8(digits, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
    auto pairs = _mm_maddubs_epi16(packed, _mm_set1_epi16(0x010A));
    const int sec = Validate ? pair(p + 17) : 10 * (p[17] - '0') + p[18] - '0';
    return {_mm_extract_epi16(pairs, 0) * 100 + _mm_extract_epi16(pairs, 1),
            _mm_extract_epi16(pairs, 2), _mm_extract_epi16(pairs, 3),
            _mm_extract_epi16(pairs, 4), _mm_extract_epi16(pairs, 5), sec};
#else
    const int hi = pair(p);
    const int lo = pair(p + 2);
    return {hi < 0 || lo < 0 ? -1 : hi * 100 + lo, pair(p + 5), pair(p + 8), pair(p + 11),
            pair(p + 14), pair(p + 17)};
#endif
}

inline int64_t civil_seconds(Clock clock) {
    auto year = years[clock.year];
    auto days = (year >> 1) + months[clock.month] + (clock.month > 2 && (year & 1)) + clock.day - 1;
    return int64_t{days} * 86400 + clock.hour * 3600 + clock.minute * 60 + clock.second;
}

#ifdef __AVX2__
inline std::optional<int64_t> epoch_13(const char* p) {
    auto start = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p));
    auto end = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p + 5));
    auto bytes = _mm_or_si128(start, _mm_slli_si128(_mm_srli_si128(end, 3), 8));
    auto digits = _mm_sub_epi8(bytes, _mm_set1_epi8('0'));
    auto nine = _mm_set1_epi8(9);
    auto valid = _mm_cmpeq_epi8(_mm_max_epu8(digits, nine), nine);
    if ((_mm_movemask_epi8(valid) & 0x1FFF) != 0x1FFF) return std::nullopt;
    auto pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010A));
    auto groups = _mm_madd_epi16(pairs, _mm_set1_epi32(0x00010064));
    auto first = static_cast<int64_t>(_mm_cvtsi128_si32(groups)) * 10000 + _mm_extract_epi32(groups, 1);
    return (first * 10000 + _mm_extract_epi32(groups, 2)) * 10 + (p[12] - '0');
}
#endif
} // namespace time_detail

inline std::optional<int64_t> timestamp_ms(std::string_view field) {
    using namespace time_detail;
    if (field.empty() || field.front() < '0' || field.front() > '9') return std::nullopt;
    if (field.size() < 5 || field[4] != '-') {
#ifdef __AVX2__
        if (field.size() == 13) return epoch_13(field.data());
#endif
        int64_t value = 0;
        auto result = std::from_chars(field.data(), field.data() + field.size(), value);
        if (result.ec == std::errc{} && result.ptr == field.data() + field.size()) return value;
        return std::nullopt;
    }
    if ((field.size() != 20 && field.size() != 25) || field[7] != '-' ||
        field[10] != 'T' || field[13] != ':' || field[16] != ':') return std::nullopt;
    auto clock = decode_clock<true>(field.data());
    if (clock.year < 1 || clock.month < 1 || clock.month > 12 ||
        clock.day < 1 || clock.day > lengths[clock.month] + (clock.month == 2 && (years[clock.year] & 1)) ||
        clock.hour < 0 || clock.hour > 23 || clock.minute < 0 || clock.minute > 59 ||
        clock.second < 0 || clock.second > 59) return std::nullopt;
    int offset_minutes = 0;
    if (field.size() == 20) {
        if (field[19] != 'Z') return std::nullopt;
    } else {
        const char sign = field[19];
        const int hours = pair(field.data() + 20);
        const int minutes = pair(field.data() + 23);
        if ((sign != '+' && sign != '-') || field[22] != ':' ||
            hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return std::nullopt;
        offset_minutes = (hours * 60 + minutes) * (sign == '-' ? -1 : 1);
    }
    return (civil_seconds(clock) - offset_minutes * 60) * 1000;
}

// The input contract guarantees valid UTC ingest timestamps with whole seconds.
inline int64_t ingest_seconds(std::string_view field) {
    return time_detail::civil_seconds(time_detail::decode_clock<false>(field.data()));
}

// Parses an event ts given its row's ingest_ts, which is always valid F1.
// When ts is F1 on the same calendar date, that date is already known to be
// valid, so only the time of day is decoded and checked.
inline std::optional<int64_t> event_timestamp_ms(std::string_view field, std::string_view ingest,
                                                 int64_t ingest_seconds) {
    if (field.size() == 20 && field[19] == 'Z' && std::memcmp(field.data(), ingest.data(), 11) == 0) {
        using time_detail::pair;
        const char* p = field.data();
        const int hour = pair(p + 11), minute = pair(p + 14), second = pair(p + 17);
        if (p[13] != ':' || p[16] != ':' || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
            second < 0 || second > 59) return std::nullopt;
        const int64_t ingest_of_day = ((ingest_seconds % 86400) + 86400) % 86400;
        return (ingest_seconds - ingest_of_day + hour * 3600 + minute * 60 + second) * 1000;
    }
    return timestamp_ms(field);
}

