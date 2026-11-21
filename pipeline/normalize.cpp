#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <optional>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

constexpr std::size_t max_fields = 7;
constexpr int encoded_csv = 10;

bool parse(std::string_view line, std::array<std::string_view, max_fields>& fields,
           std::size_t width) {
    for (std::size_t index = 0; index < width; ++index) {
        const auto comma = line.find(',');
        if ((comma == std::string_view::npos) != (index + 1 == width)) {
            return false;
        }
        auto field = line.substr(0, comma);
        if (field.size() >= 2 && field.front() == '"' && field.back() == '"') {
            field.remove_prefix(1);
            field.remove_suffix(1);
        }
        if (field.find('"') != std::string_view::npos) {
            return false;
        }
        fields[index] = field;
        if (comma != std::string_view::npos) {
            line.remove_prefix(comma + 1);
        }
    }
    return !fields[0].empty();
}

// Unlike the general CSV fields, timestamps contain only ASCII syntax.
int digits(std::string_view part) {
    int number = 0;
    for (char byte : part) {
        if (byte < '0' || byte > '9') {
            return -1;
        }
        number = number * 10 + (byte - '0');
    }
    return number;
}

std::optional<std::int64_t> timestamp_ms(std::string_view field) {
    constexpr std::int64_t second_ms = 1000;
    constexpr std::int64_t day_ms = 86400 * second_ms;
    if (field.empty() || field.front() < '0' || field.front() > '9') {
        return std::nullopt;
    }
    if (field.size() < 5 || field[4] != '-') {
        std::int64_t value = 0;
        auto result = std::from_chars(field.data(), field.data() + field.size(), value);
        if (result.ec == std::errc{} && result.ptr == field.data() + field.size()) {
            return value;
        }
        return std::nullopt;
    }

    if ((field.size() != 20 && field.size() != 25) || field[7] != '-' ||
        field[10] != 'T' || field[13] != ':' || field[16] != ':') {
        return std::nullopt;
    }
    const int year = digits(field.substr(0, 4));
    const int month = digits(field.substr(5, 2));
    const int day = digits(field.substr(8, 2));
    const int hour = digits(field.substr(11, 2));
    const int minute = digits(field.substr(14, 2));
    const int second = digits(field.substr(17, 2));
    if (year < 1 || month < 1 || month > 12 || day < 1 || day > 31 ||
        hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
        return std::nullopt;
    }
    const std::chrono::year_month_day date{
        std::chrono::year(year), std::chrono::month(month), std::chrono::day(day)};
    if (!date.ok()) {
        return std::nullopt;
    }

    int offset_minutes = 0;
    if (field.size() == 20) {
        if (field[19] != 'Z') {
            return std::nullopt;
        }
    } else {
        const char sign = field[19];
        const int hours = digits(field.substr(20, 2));
        const int minutes = digits(field.substr(23, 2));
        if ((sign != '+' && sign != '-') || field[22] != ':' ||
            hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
            return std::nullopt;
        }
        offset_minutes = (hours * 60 + minutes) * (sign == '-' ? -1 : 1);
    }

    const auto days = std::chrono::sys_days(date).time_since_epoch().count();
    return days * day_ms + ((hour * 60 + minute - offset_minutes) * 60 + second) * second_ms;
}

bool is_timestamp(std::size_t width, std::size_t index) {
    return (width == 7 && (index == 2 || index == 3)) || (width == 4 && index == 3);
}

int normalize(const char* source, const char* target, std::size_t width) {
    std::ifstream input(source, std::ios::binary);
    std::ofstream output(target, std::ios::binary);
    if (!input || !output) {
        throw std::runtime_error("cannot open input or output");
    }

    std::string line;
    std::string normalized;
    std::array<std::string_view, max_fields> fields;
    bool encoded = false;
    std::getline(input, line); // Each part begins with a header.
    for (std::size_t index = 0; index < width; ++index) {
        output << (index ? ",field" : "field") << index;
    }
    output << '\n';

    while (std::getline(input, line)) {
        if (!parse(line, fields, width)) {
            continue;
        }
        normalized.clear();
        for (std::size_t index = 0; index < width; ++index) {
            if (index) {
                normalized += ',';
            }
            const auto field = fields[index];
            if (is_timestamp(width, index)) {
                if (auto value = timestamp_ms(field)) {
                    char buffer[24];
                    auto result = std::to_chars(buffer, buffer + sizeof(buffer), *value);
                    normalized.append(buffer, result.ptr);
                }
                continue;
            }
            if (field.find_first_of("%\r") == std::string_view::npos) {
                normalized.append(field);
                continue;
            }
            encoded = true;
            for (const char byte : field) {
                switch (byte) {
                    case '%': normalized += "%25"; break;
                    case '\r': normalized += "%0D"; break;
                    default: normalized += byte;
                }
            }
        }
        normalized += '\n';
        output.write(normalized.data(), normalized.size());
    }
    output.close();
    if (input.bad() || !output) {
        throw std::runtime_error("I/O error");
    }
    return encoded ? encoded_csv : 0;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: normalize INPUT OUTPUT FIELD_COUNT\n";
        return 1;
    }
    std::string_view count(argv[3]);
    std::size_t width = 0;
    auto result = std::from_chars(count.begin(), count.end(), width);
    if (result.ec != std::errc{} || result.ptr != count.end() || width < 1 || width > max_fields) {
        std::cerr << "invalid field count\n";
        return 1;
    }
    try {
        return normalize(argv[1], argv[2], width);
    } catch (const std::exception& error) {
        std::cerr << argv[1] << ": " << error.what() << '\n';
        return 1;
    }
}
