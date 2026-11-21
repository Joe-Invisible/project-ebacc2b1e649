#include "timestamp.hpp"

#include <array>
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
