#pragma once

#include "native_input.hpp"
#include <fstream>
#include <limits>

#if __has_include(<lz4.h>)
#include <lz4.h>
#else
// These two C ABI entry points are available in the Linux liblz4 runtime.
extern "C" int LZ4_compressBound(int size);
extern "C" int LZ4_compress_default(const char* input, char* output, int size, int capacity);
#endif

namespace shopping {

struct UserDay {
    Key person;
    int32_t day;
    uint32_t category = 0;
    int64_t sessions = 0;
    int64_t events = 0;
    int64_t revenue = 0;
};

struct Campaign {
    std::string id;
    std::string channel;
};

struct CampaignMetrics {
    int64_t sessions = 0;
    int64_t purchasing = 0;
    int64_t revenue = 0;
    int64_t assisted = 0;

    CampaignMetrics& operator+=(const CampaignMetrics& other) {
        sessions += other.sessions;
        purchasing += other.purchasing;
        revenue += other.revenue;
        assisted += other.assisted;
        return *this;
    }
};

namespace parquet {
// File metadata follows parquet-format/src/main/thrift/parquet.thrift.
// Each flat column has one LZ4_RAW-compressed PLAIN data page per row group.
enum Physical { integer = 1, bigint = 2, string = 6 };
enum Annotation { none = -1, utf8 = 0, date = 6 };
struct Field {
    std::string_view name;
    Physical type;
    Annotation annotation = none;
    bool nullable = false;
};

inline constexpr std::array user_schema{
    Field{"user_id", string, utf8}, Field{"day", integer, date}, Field{"sessions", bigint},
    Field{"events", bigint}, Field{"revenue_cents", bigint}, Field{"top_category", string, utf8, true},
};
inline constexpr std::array campaign_schema{
    Field{"campaign_id", string, utf8}, Field{"channel", string, utf8}, Field{"day", integer, date},
    Field{"sessions", bigint}, Field{"purchasing_sessions", bigint}, Field{"revenue_cents", bigint},
    Field{"assisted_sessions", bigint},
};

using Bytes = std::vector<char>;

inline void varint(Bytes& out, uint64_t value) {
    while (value >= 128) {
        out.push_back(static_cast<char>((value & 127) | 128));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

template<class Int>
void little_endian(Bytes& out, Int value) {
    static_assert(std::endian::native == std::endian::little);
    auto offset = out.size();
    out.resize(offset + sizeof(value));
    std::memcpy(out.data() + offset, &value, sizeof(value));
}

struct Compact {
    enum Type { i32 = 5, i64 = 6, binary = 8, list = 9, structure = 12 };
    Bytes& out;
    int previous = 0;

    void field(int id, Type type) {
        // The schema below writes fields in order with deltas in [1,15].
        out.push_back(static_cast<char>(((id - previous) << 4) | type));
        previous = id;
    }
    void number(int64_t value) {
        varint(out, (static_cast<uint64_t>(value) << 1) ^ (value < 0 ? UINT64_MAX : 0));
    }
    void scalar(int id, Type type, int64_t value) {
        field(id, type);
        number(value);
    }
    void text(std::string_view value) {
        varint(out, value.size());
        out.insert(out.end(), value.begin(), value.end());
    }
    void text(int id, std::string_view value) {
        field(id, binary);
        text(value);
    }
    void array(int id, Type type, size_t length) {
        field(id, list);
        out.push_back(static_cast<char>((std::min<size_t>(15, length) << 4) | type));
        if (length >= 15) varint(out, length);
    }
    void stop() { out.push_back(0); }
};

class File {
    static constexpr size_t rows_per_group = 32768;
    struct ColumnPage {
        int64_t offset, bytes, raw_bytes;
    };
    struct Group {
        size_t rows;
        std::vector<ColumnPage> pages;
    };
    struct Column {
        Bytes values;
        Bytes definitions;
    };

    std::ofstream file;
    const Field* schema;
    size_t count;
    std::vector<Column> columns;
    std::vector<Group> groups;
    Bytes packed, compressed;
    size_t rows = 0;
    int64_t total_rows = 0;
    int64_t offset = 0;

    void write(std::string_view bytes) {
        if (bytes.empty()) return;
        file.write(bytes.data(), bytes.size());
        offset += bytes.size();
    }

    void write(const Bytes& bytes) {
        if (!bytes.empty()) write(std::string_view(bytes.data(), bytes.size()));
    }

    void definition(size_t column, bool valid) {
        if (!schema[column].nullable) return;
        auto& bits = columns[column].definitions;
        if (rows % 8 == 0) bits.push_back(0);
        if (valid) bits.back() |= static_cast<char>(1U << (rows % 8));
    }

    void flush_group() {
        if (!rows) return;
        auto& group = groups.emplace_back(Group{rows, {}});
        for (size_t i = 0; i < count; ++i) {
            auto& column = columns[i];
            Bytes run_header, levels_prefix;
            if (schema[i].nullable) {
                // One bit-packed hybrid run with one bit per definition.
                varint(run_header, (column.definitions.size() << 1) | 1);
                little_endian(levels_prefix, static_cast<uint32_t>(run_header.size() + column.definitions.size()));
            }
            auto payload = column.values.size() + column.definitions.size() + run_header.size() + levels_prefix.size();
            if (payload > std::numeric_limits<int32_t>::max()) throw std::runtime_error("Parquet page is too large");
            const char* source = column.values.data();
            if (schema[i].nullable) {
                packed.clear();
                for (const auto* part : {&levels_prefix, &run_header, &column.definitions, &column.values}) {
                    packed.insert(packed.end(), part->begin(), part->end());
                }
                source = packed.data();
            }
            int bound = LZ4_compressBound(payload);
            if (!bound) throw std::runtime_error("Parquet LZ4 block is too large");
            compressed.resize(bound);
            int compressed_size = LZ4_compress_default(source, compressed.data(), payload, bound);
            if (!compressed_size) throw std::runtime_error("Parquet LZ4 failed");
            Bytes header;
            Compact page{header};
            page.scalar(1, Compact::i32, 0);
            page.scalar(2, Compact::i32, payload);
            page.scalar(3, Compact::i32, compressed_size);
            page.field(5, Compact::structure);
            Compact data{header};
            data.scalar(1, Compact::i32, rows);
            data.scalar(2, Compact::i32, 0); // PLAIN
            data.scalar(3, Compact::i32, 3); // RLE definitions
            data.scalar(4, Compact::i32, 3); // RLE repetitions
            data.stop();
            page.stop();
            group.pages.push_back({offset, static_cast<int64_t>(header.size() + compressed_size),
                                   static_cast<int64_t>(header.size() + payload)});
            write(header);
            write(std::string_view(compressed.data(), compressed_size));
            column.definitions.clear();
            column.values.clear();
        }
        total_rows += rows;
        rows = 0;
    }

public:
    template<size_t N>
    File(const fs::path& path, const std::array<Field, N>& fields)
        : file(path, std::ios::binary), schema(fields.data()), count(N), columns(N) {
        file.exceptions(std::ios::badbit | std::ios::failbit);
        write("PAR1");
    }
    template<class Int>
    void value(size_t column, Int value) {
        definition(column, true);
        little_endian(columns[column].values, value);
    }
    void text(size_t column, std::string_view text) {
        if (text.size() > std::numeric_limits<int32_t>::max()) throw std::runtime_error("Parquet string is too large");
        value(column, static_cast<uint32_t>(text.size()));
        auto& data = columns[column].values;
        data.insert(data.end(), text.begin(), text.end());
    }
    void null(size_t column) { definition(column, false); }
    void finish_row() {
        if (++rows == rows_per_group) flush_group();
    }
    void finish() {
        flush_group();
        Bytes footer;
        Compact metadata{footer};
        metadata.scalar(1, Compact::i32, 1);
        metadata.array(2, Compact::structure, count + 1);
        Compact root{footer};
        root.text(4, "schema");
        root.scalar(5, Compact::i32, count);
        root.stop();
        for (size_t i = 0; i < count; ++i) {
            const auto& field = schema[i];
            Compact column{footer};
            column.scalar(1, Compact::i32, field.type);
            column.scalar(3, Compact::i32, field.nullable ? 1 : 0);
            column.text(4, field.name);
            if (field.annotation != none) column.scalar(6, Compact::i32, field.annotation);
            column.stop();
        }
        metadata.scalar(3, Compact::i64, total_rows);
        metadata.array(4, Compact::structure, groups.size());
        for (const auto& group : groups) {
            Compact rows_meta{footer};
            rows_meta.array(1, Compact::structure, count);
            int64_t total = 0;
            for (size_t i = 0; i < count; ++i) {
                const auto& page = group.pages[i];
                const auto& field = schema[i];
                Compact chunk{footer};
                chunk.scalar(2, Compact::i64, 0);
                chunk.field(3, Compact::structure);
                Compact column{footer};
                column.scalar(1, Compact::i32, field.type);
                column.array(2, Compact::i32, field.nullable ? 2 : 1);
                column.number(0);
                if (field.nullable) column.number(3);
                column.array(3, Compact::binary, 1);
                column.text(field.name);
                column.scalar(4, Compact::i32, 7); // LZ4_RAW
                column.scalar(5, Compact::i64, group.rows);
                column.scalar(6, Compact::i64, page.raw_bytes);
                column.scalar(7, Compact::i64, page.bytes);
                column.scalar(9, Compact::i64, page.offset);
                column.stop();
                chunk.stop();
                total += page.raw_bytes;
            }
            rows_meta.scalar(2, Compact::i64, total);
            rows_meta.scalar(3, Compact::i64, group.rows);
            rows_meta.stop();
        }
        metadata.stop();
        if (footer.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("Parquet footer is too large");
        }
        write(footer);
        Bytes length;
        little_endian(length, static_cast<uint32_t>(footer.size()));
        write(length);
        write("PAR1");
        file.close();
    }
};

inline void append_user(File& writer, const UserDay& row, const Keys& keys, const std::vector<std::string>& categories) {
    std::array<char, 9> buffer;
    writer.text(0, keys.decode(row.person, buffer));
    writer.value(1, row.day);
    writer.value(2, row.sessions);
    writer.value(3, row.events);
    writer.value(4, row.revenue);
    if (row.revenue) writer.text(5, categories[row.category]);
    else writer.null(5);
    writer.finish_row();
}
} // namespace parquet

inline void write_campaigns(const fs::path& output, const std::vector<Campaign>& campaigns,
                            const std::vector<CampaignMetrics>& metrics, int32_t first_day) {
    auto campaign_output = output / "campaign_daily";
    fs::create_directories(campaign_output);
    parquet::File writer(campaign_output / "part-0.parquet", parquet::campaign_schema);
    for (size_t slot = 0; slot < metrics.size(); ++slot) {
        const auto& metric = metrics[slot];
        if (!metric.sessions && !metric.assisted) continue;
        const auto& campaign = campaigns[slot % campaigns.size()];
        writer.text(0, campaign.id);
        writer.text(1, campaign.channel);
        writer.value(2, first_day + static_cast<int32_t>(slot / campaigns.size()));
        writer.value(3, metric.sessions);
        writer.value(4, metric.purchasing);
        writer.value(5, metric.revenue);
        writer.value(6, metric.assisted);
        writer.finish_row();
    }
    writer.finish();
}

} // namespace shopping
