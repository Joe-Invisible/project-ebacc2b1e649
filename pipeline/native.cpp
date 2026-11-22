#include "native_input.hpp"
#include "timestamp.hpp"
#include "native_output.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <sys/resource.h>
#include <type_traits>

extern "C" void sampler_dump() __attribute__((weak));

namespace shopping {
constexpr int64_t day_ms = 86400000;
constexpr int64_t week_ms = 7 * day_ms;
constexpr uint32_t session_gap = 1800000;
constexpr size_t event_bucket_count = 256;
constexpr size_t person_bucket_count = 512;
constexpr size_t input_chunk = 2 * 1024 * 1024;
// Event and click files are read in chunks that stay in the L2 cache.
constexpr size_t read_chunk = 256 * 1024;
constexpr uint32_t invalid_event = std::numeric_limits<uint32_t>::max();
constexpr uint32_t missing_campaign = std::numeric_limits<uint32_t>::max();
constexpr uint64_t price_mask = (uint64_t{1} << 24) - 1;

int32_t utc_day(int64_t value) {
    return value / day_ms - (value % day_ms < 0);
}

struct WideEventLayout {};
// One record per parsed event copy, partitioned by event ID. Invalid copies
// are kept with ts == invalid_event because they can still win deduplication.
struct EventRow {
    uint64_t id_hash;
    uint64_t user_hash;
    uint32_t ingest;   // Ingest seconds relative to the parse origin.
    uint32_t ts;       // Offset from the window start, or invalid_event.
    uint32_t purchase; // Low 24 bits are price in cents, high 8 bits are category.
    uint32_t unused;
};
static_assert(sizeof(EventRow) == 32);
// A surviving event, partitioned by person.
struct PersonEvent {
    uint64_t person_hash;
    uint32_t ts;
    uint32_t purchase;
};
static_assert(sizeof(PersonEvent) == 16);
// A valid click, partitioned by person.
struct RawClick {
    uint64_t person_hash;
    Key id;
    uint32_t ts; // Offset from the beginning of the click window.
    uint32_t campaign;
    uint64_t padding; // Two rows fill a cache line for streaming stores.
};

#ifdef __AVX2__
// Buffer a full cache line before writing to the long-lived event arrays.
template<class T>
class EventBuffer {
    static constexpr size_t rows_per_line = 64 / sizeof(T);
    static_assert((sizeof(T) == 16 || sizeof(T) == 32) && std::is_trivially_copyable_v<T>);

    T* rows = nullptr;
    size_t used = 0, capacity = 0;
    bool owned = true;
    alignas(64) T line[rows_per_line];

public:
    EventBuffer() = default;
    EventBuffer(const EventBuffer&) = delete;
    EventBuffer& operator=(const EventBuffer&) = delete;
    ~EventBuffer() {
        if (owned) std::free(rows);
    }

    // Use 64-byte aligned memory owned by an Arena until the buffer outgrows it.
    void attach(T* memory, size_t rows_capacity) {
        rows = memory;
        capacity = rows_capacity & ~(rows_per_line - 1);
        owned = false;
    }

    size_t size() const { return used; }
    const T* begin() const { return rows; }
    const T* end() const { return used ? rows + used : rows; }
    T& operator[](size_t index) { return rows[index]; }

    void reserve(size_t minimum) {
        if (minimum <= capacity) return;
        constexpr auto maximum = (SIZE_MAX / sizeof(T)) & ~(rows_per_line - 1);
        if (minimum > maximum) throw std::length_error("too many staged events");
        const auto rounded = (minimum + rows_per_line - 1) & ~(rows_per_line - 1);
        auto* allocation = static_cast<T*>(std::aligned_alloc(64, rounded * sizeof(T)));
        if (!allocation) throw std::bad_alloc{};
        if (rows) {
            // Growth reads the completed lines from the old allocation.
            _mm_sfence();
            std::memcpy(allocation, rows, (used & ~(rows_per_line - 1)) * sizeof(T));
            if (owned) std::free(rows);
        }
        rows = allocation;
        capacity = rounded;
        owned = true;
    }

    void push_back(const T& value) {
        const auto slot = used % rows_per_line;
        line[slot] = value;
        if (slot + 1 == rows_per_line) {
            if (used >= capacity) reserve(std::max(rows_per_line, capacity * 2));
            auto* destination = reinterpret_cast<__m256i*>(rows + used - slot);
            const auto* source = reinterpret_cast<const __m256i*>(line);
            _mm256_stream_si256(destination, _mm256_load_si256(source));
            _mm256_stream_si256(destination + 1, _mm256_load_si256(source + 1));
        }
        ++used;
    }

    void flush_tail() {
        if (used > capacity) reserve(used);
        const auto tail = used % rows_per_line;
        if (tail) std::memcpy(rows + used - tail, line, tail * sizeof(T));
    }

    // Return the pages concurrently with other workers. Freeing would unmap
    // under the exclusive address-space lock or leave pages for process exit.
    void release() {
        // Arena memory is reused by a later stage instead.
        if (owned) discard_memory(rows, capacity * sizeof(T));
        used = 0;
    }
};
#else
template<class T>
using EventBuffer = std::vector<T>;
#endif

// One reserved region holds the deduplication versions. Once they are
// consumed, the grouped timelines are carved from the same region and reuse
// the pages the versions already faulted in.
// Transparent huge pages cut page faults by 512x where the kernel can back
// them. SHOPPING_HUGEPAGES=0 turns them off.
inline bool use_huge_pages() {
    const char* setting = std::getenv("SHOPPING_HUGEPAGES");
    return !setting || std::string_view(setting) != "0";
}

class Arena {
    static constexpr size_t huge_page = size_t{2} << 20;
    char* mapping = nullptr;
    size_t mapped = 0;
    char* base = nullptr;
    size_t bytes = 0;
    std::atomic<size_t> used{0};

public:
    Arena() = default;
    Arena(const Arena&) = delete;
    ~Arena() {
        if (mapping) munmap(mapping, mapped);
    }
    void reserve(size_t size) {
        size = (size + huge_page - 1) & ~(huge_page - 1);
        // Over-allocate so the usable region starts on a huge-page boundary.
        const auto total = size + huge_page;
        auto memory = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (memory == MAP_FAILED) return;
        mapping = static_cast<char*>(memory);
        mapped = total;
        base = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(mapping) + huge_page - 1) & ~(huge_page - 1));
        bytes = size;
        if (use_huge_pages()) madvise(base, bytes, MADV_HUGEPAGE);
    }
    // Returns 64-byte aligned memory, or null once the region is full.
    void* allocate(size_t size) {
        if (!base) return nullptr;
        size = (size + 63) & ~size_t{63};
        const auto offset = used.fetch_add(size, std::memory_order_relaxed);
        return offset + size <= bytes ? base + offset : nullptr;
    }
    // Start over once nothing allocated so far is needed.
    void reset() { used.store(0, std::memory_order_relaxed); }
    void discard(size_t index, size_t tasks) const {
        const auto slice = (bytes / tasks + 4095) & ~size_t{4095};
        const auto first = std::min(bytes, index * slice);
        const auto last = std::min(bytes, first + slice);
        if (last > first) madvise(base + first, last - first, MADV_DONTNEED);
    }
};

template<class T>
using PersonBuckets = std::array<std::vector<T>, person_bucket_count>;
using EventBuckets = std::array<EventBuffer<EventRow>, event_bucket_count>;
using PersonEventRows = std::array<EventBuffer<PersonEvent>, person_bucket_count>;
using RawClickRows = std::array<EventBuffer<RawClick>, person_bucket_count>;

template<class T>
void release_event_buffer(EventBuffer<T>& buffer) {
#ifdef __AVX2__
    buffer.release();
#else
    EventBuffer<T>().swap(buffer);
#endif
}

template<size_t Count>
size_t bucket_from_hash(uint64_t hash) {
    static_assert(std::has_single_bit(Count));
    return hash >> (64 - std::bit_width(Count - 1));
}

struct PersonAliases {
    static constexpr size_t bucket_count = 64;
    std::array<Lookup<Key, true>, bucket_count> indexes;

    void prefetch(uint64_t hash) const {
        indexes[bucket_from_hash<bucket_count>(hash)].prefetch_filtered(hash);
    }

    Key get(Key user, uint64_t hash) const {
        return indexes[bucket_from_hash<bucket_count>(hash)].get_filtered(hash, hash, user);
    }
};

PersonAliases link_people(Keys& keys, const fs::path& input) {
    constexpr size_t bucket_count = PersonAliases::bucket_count;
    MappedInput links;
    links.folder(input / "links", 256 * 1024);
    struct Endpoint {
        Key hash;
        uint32_t edge_and_side;
    };
    struct Edge { uint32_t first, second; };
    // Partition only the endpoints. Keep edges in input order for the union pass.
    std::array<std::vector<Endpoint>, bucket_count> endpoints;
    std::vector<Edge> edges;
    for (auto part : links.parts) {
        read_part<2>(part, [&](const auto& fields) {
            if (fields[1].empty()) return;
            const auto first = keys.encode(fields[0]), second = keys.encode(fields[1]);
            if (first == second) return;
            // One bit encodes which end of the edge this is. The same bound
            // keeps the number of distinct nodes within 32 bits.
            if (edges.size() >= (UINT32_MAX >> 1)) throw std::runtime_error("too many links");
            const auto position = static_cast<uint32_t>(edges.size() << 1);
            edges.emplace_back();
            const auto first_hash = hash_key(first), second_hash = hash_key(second);
            endpoints[bucket_from_hash<bucket_count>(first_hash)].push_back({first_hash, position});
            endpoints[bucket_from_hash<bucket_count>(second_hash)].push_back({second_hash, position | 1});
        });
    }

    // Give each distinct ID a dense node number while its hash shard is hot.
    std::vector<Key> nodes;
    nodes.reserve(edges.size());
    PersonAliases aliases;
    for (size_t bucket = 0; bucket < bucket_count; ++bucket) {
        auto& ids = endpoints[bucket];
        const auto first_node = nodes.size();
        auto& index = aliases.indexes[bucket];
        index = Lookup<Key, true>((ids.size() + 1) / 2);
        for (const auto& endpoint : ids) {
            const auto next = static_cast<uint32_t>(nodes.size());
            const auto node = static_cast<uint32_t>(index.find_or_insert(endpoint.hash, next));
            if (node == next) {
                nodes.push_back(unhash_key(endpoint.hash));
                const auto unique = nodes.size() - first_node;
                if (unique * 4 > index.capacity() * 3) index.reserve(unique);
            }
            auto& edge = edges[endpoint.edge_and_side >> 1];
            if (endpoint.edge_and_side & 1) edge.second = node;
            else edge.first = node;
        }
        std::vector<Endpoint>().swap(ids);
    }

    auto parents = std::make_unique_for_overwrite<uint32_t[]>(nodes.size());
    for (uint32_t node = 0; node < nodes.size(); ++node) parents[node] = node;
    auto root = [&](uint32_t node) {
        for (;;) {
            auto parent = parents[node];
            if (node == parent) return node;
            auto ancestor = parents[parent];
            parents[node] = ancestor;
            node = ancestor;
        }
    };
    // Attach the larger root to the smaller root to preserve byte-wise minima.
    for (auto [first, second] : edges) {
        auto a = root(first), b = root(second);
        if (a != b) {
            if (keys.less(nodes[b], nodes[a])) std::swap(a, b);
            parents[b] = a;
        }
    }
    // Reuse the node index for alias lookups. Canonical IDs and absent IDs
    // normally miss the bitmap and can skip probing the table.
    for (auto& index : aliases.indexes) {
        index.index_updated_values([&](Key& value) {
            const auto node = static_cast<uint32_t>(value);
            const auto representative = root(node);
            value = nodes[representative];
            return node != representative;
        });
    }
    return aliases;
}

struct Dimensions {
    Lookup<uint32_t> products;
    Lookup<uint32_t> campaigns;
    std::vector<std::string> categories{"unknown"};
    std::vector<Campaign> campaign_values;
};

Dimensions dimensions(Keys& keys, const fs::path& input) {
    Dimensions result;
    std::vector<std::pair<Key, uint32_t>> products;
    std::unordered_map<std::string, uint32_t> categories;
    MappedInput product_input;
    product_input.add(input / "products.csv", input_chunk);
    for (auto part : product_input.parts) read_part<2>(part, [&](const auto& fields) {
        if (fields[1].empty()) return;
        auto [entry, inserted] = categories.try_emplace(std::string(fields[1]), result.categories.size());
        if (inserted) result.categories.push_back(entry->first);
        products.emplace_back(keys.encode(fields[0]), entry->second);
    });
    result.products = Lookup<uint32_t>(products.size());
    for (auto [id, category] : products) result.products.insert(id, category);

    MappedInput campaign_input;
    campaign_input.add(input / "campaigns.csv", input_chunk);
    for (auto part : campaign_input.parts) read_part<2>(part, [&](const auto& fields) {
        result.campaign_values.push_back({std::string(fields[0]), std::string(fields[1])});
    });
    result.campaigns = Lookup<uint32_t>(result.campaign_values.size());
    for (uint32_t i = 0; i < result.campaign_values.size(); ++i) {
        result.campaigns.insert(keys.encode(result.campaign_values[i].id), i);
    }
    result.campaign_values.push_back({"organic", "organic"});
    return result;
}

uint32_t parse_price(std::string_view text) {
    if (text.empty() || text.front() < '0' || text.front() > '9') return 0;
    uint32_t value = 0;
    auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size() && value <= 10000000 ? value : 0;
}

size_t estimate_event_count(const MappedInput& mapped) {
    size_t bytes = 0;
    for (const auto& part : mapped.parts) bytes += part.end - part.first;
    size_t sampled_bytes = 0, sampled_rows = 0;
    const size_t interval = std::max<size_t>(1, mapped.parts.size() / 16);
    for (size_t i = 0; i < mapped.parts.size(); i += interval) {
        auto part = align_part(mapped.parts[i]);
        auto line = part.first;
        for (size_t n = 0; n < 128 && line < part.end; ++n) {
            line = next_line(line, part.end);
            ++sampled_rows;
        }
        sampled_bytes += line - part.first;
    }
    if (!sampled_rows) return 0;
    // Sampled density controls capacity only. Malformed short rows do not
    // inflate the reservation beyond the minimum possible event width.
    const auto average = std::max(27.0, static_cast<double>(sampled_bytes) / sampled_rows);
    return static_cast<size_t>(bytes / average);
}

void read_events(const ChunkedInput& mapped, size_t workers, int64_t start, int64_t end,
                 Keys& keys, const Dimensions& dims, std::vector<EventBuckets>& output, Arena& arena) {
    const auto origin = start / 1000 - (int64_t{1} << 31);
    const auto rows_per_worker = mapped.estimate_rows(27) / workers;
    const auto reserved = rows_per_worker + rows_per_worker / 8;
    const auto readers = std::min(workers, mapped.chunks.size());
    const bool reserve = reserved >= 256 * event_bucket_count;
    const auto row_slot = (reserved / event_bucket_count + 1) & ~size_t{1};
    if (reserve) arena.reserve(readers * event_bucket_count * row_slot * sizeof(EventRow));
    std::atomic<size_t> next_part{0};
    parallel(readers, readers, [&](size_t, size_t worker) {
#ifdef __AVX2__
        // Every producer publishes its streaming writes before the join,
        // including on an exceptional exit.
        struct WriteFence {
            ~WriteFence() { _mm_sfence(); }
        } fence_on_exit;
#endif
        try {
            if (reserve) {
                for (auto& bucket : output[worker]) {
#ifdef __AVX2__
                    if (auto* memory = arena.allocate(row_slot * sizeof(EventRow))) {
                        bucket.attach(static_cast<EventRow*>(memory), row_slot);
                        continue;
                    }
#endif
                    bucket.reserve(row_slot);
                }
            }
            std::vector<char> buffer;
            // Rows are handled in small batches: every ID lookup in a batch is
            // hashed and its table slot prefetched before any row needs it.
            constexpr size_t batch_size = 16;
            std::array<std::array<std::string_view, 7>, batch_size> batch;
            std::array<Keys::Prepared, batch_size> event_ids, user_ids;
            size_t batched = 0;
            auto flush = [&] {
                for (size_t row = 0; row < batched; ++row) {
                    event_ids[row] = keys.prepare(batch[row][0]);
                    user_ids[row] = keys.prepare(batch[row][1]);
                }
                for (size_t row = 0; row < batched; ++row) {
                    const auto& fields = batch[row];
                    if (fields[3].size() != 20) throw std::runtime_error("invalid event ingest_ts");
                    const auto ingest = ingest_seconds(fields[3]);
                    const auto order = static_cast<uint64_t>(ingest) - static_cast<uint64_t>(origin);
                    if (order > UINT32_MAX) throw WideEventLayout{};
                    auto ts = timestamp_ms(fields[2]);
                    const auto id_hash = hash_key(keys.resolve(event_ids[row]));
                    EventRow copy{id_hash, 0, static_cast<uint32_t>(order), invalid_event, 0, 0};
                    if (ts && *ts >= start && *ts < end && ingest * 1000 - *ts <= day_ms &&
                        !fields[1].empty() && !fields[5].empty()) {
                        const auto offset = static_cast<uint32_t>(*ts - start);
                        if (fields[4] == "purchase") {
                            if (auto price = parse_price(fields[6])) {
                                auto category = dims.products.get(keys.encode(fields[5]), 0);
                                copy.user_hash = hash_key(keys.resolve(user_ids[row]));
                                copy.ts = offset;
                                copy.purchase = price | (category << 24);
                            }
                        } else if (fields[4] == "view" || fields[4] == "cart") {
                            copy.user_hash = hash_key(keys.resolve(user_ids[row]));
                            copy.ts = offset;
                        }
                    }
                    output[worker][bucket_from_hash<event_bucket_count>(id_hash)].push_back(copy);
                }
                batched = 0;
            };
            for (size_t part; (part = next_part.fetch_add(1, std::memory_order_relaxed)) < mapped.chunks.size();) {
                read_part<7>(mapped.load(part, buffer), [&](const auto& fields) {
                    batch[batched] = fields;
                    if (++batched == batch_size) flush();
                });
                // The batch points into the chunk buffer, which the next load reuses.
                flush();
            }
#ifdef __AVX2__
            for (auto& bucket : output[worker]) bucket.flush_tail();
#endif
        } catch (...) {
            next_part.store(mapped.chunks.size(), std::memory_order_relaxed);
            throw;
        }
    });
}

// Resolves each surviving event's person and routes it to a person bucket.
// Alias lookups miss the cache, so each one is prefetched a few rows ahead.
class PersonRouter {
    static constexpr size_t depth = 16;
    const PersonAliases& aliases;
    PersonEventRows& output;
    std::array<const EventRow*, depth> pending{};
    size_t head = 0, count = 0;

    void route(const EventRow& row) {
        const auto user = unhash_key(row.user_hash);
        const auto person = aliases.get(user, row.user_hash);
        const auto hash = person == user ? row.user_hash : hash_key(person);
        output[bucket_from_hash<person_bucket_count>(hash)].push_back({hash, row.ts, row.purchase});
    }

public:
    PersonRouter(const PersonAliases& aliases, PersonEventRows& output) : aliases(aliases), output(output) {}

    void push(const EventRow& row) {
        aliases.prefetch(row.user_hash);
        if (count == depth) {
            route(*pending[head]);
            pending[head] = &row;
            head = (head + 1) % depth;
        } else {
            pending[(head + count++) % depth] = &row;
        }
    }

    void flush() {
        for (; count; --count, head = (head + 1) % depth) route(*pending[head]);
        head = 0;
    }
};

// Keeps, for each event ID, the copy with the smallest ingest time. Valid
// winners go straight to their person's bucket.
void deduplicate(std::vector<EventBuckets>& events, size_t workers, const PersonAliases& aliases,
                 std::vector<PersonEventRows>& people) {
    parallel(workers, event_bucket_count, [&](size_t worker, size_t bucket) {
#ifdef __AVX2__
        // Publish this task's streaming writes to the person buckets.
        struct WriteFence {
            ~WriteFence() { _mm_sfence(); }
        } fence_on_exit;
#endif
        size_t count = 0;
        for (const auto& feed : events) count += feed[bucket].size();
        if (!count) return;
        const size_t homes = std::bit_ceil(std::max<size_t>(64, count * 8));
        std::vector<uint64_t> seen(homes / 64), repeated(homes / 64);
        size_t collisions = 0;
        for (const auto& feed : events) {
            for (const auto& row : feed[bucket]) {
                const auto home = row.id_hash & (homes - 1);
                const auto bit = uint64_t{1} << (home % 64);
                auto& word = seen[home / 64];
                if (word & bit) {
                    repeated[home / 64] |= bit;
                    ++collisions;
                }
                word |= bit;
            }
        }
        // An ID whose home bit was set once has a single copy, which wins.
        // The others keep the copy with the smallest ingest time; the table
        // holds each ID hash next to its winner to avoid chasing pointers.
        struct Winner {
            uint64_t id_hash;
            const EventRow* row;
        };
        const size_t capacity = std::bit_ceil(std::max<size_t>(2, collisions * 4));
        std::vector<Winner> winners(capacity, Winner{0, nullptr});
        PersonRouter router(aliases, people[worker]);
        for (const auto& feed : events) {
            for (const auto& row : feed[bucket]) {
                const auto hash = row.id_hash;
                const auto home = hash & (homes - 1);
                if (!(repeated[home / 64] & (uint64_t{1} << (home % 64)))) {
                    if (row.ts != invalid_event) router.push(row);
                    continue;
                }
                auto slot = hash & (capacity - 1);
                while (winners[slot].row && winners[slot].id_hash != hash) slot = (slot + 1) & (capacity - 1);
                auto& winner = winners[slot];
                if (!winner.row) winner = {hash, &row};
                else if (row.ingest < winner.row->ingest) winner.row = &row;
            }
        }
        for (const auto& winner : winners) {
            if (winner.row && winner.row->ts != invalid_event) router.push(*winner.row);
        }
        router.flush();
        for (auto& feed : events) release_event_buffer(feed[bucket]);
    });
}

size_t click_slot(const ChunkedInput& mapped, size_t workers) {
    const auto per_bucket = mapped.estimate_rows(27) / workers / person_bucket_count;
    return (per_bucket + per_bucket / 4 + 1) & ~size_t{1};
}

// Clicks are routed by person as they are parsed, so they need the linked IDs.
void read_clicks(const ChunkedInput& mapped, size_t workers, int64_t start, int64_t end, Keys& keys,
                 const Dimensions& dims, const PersonAliases& aliases, std::vector<RawClickRows>& output,
                 Arena& arena, std::atomic<size_t>& next, size_t worker_offset) {
    // Growing the per-bucket buffers would copy rows and touch extra pages.
    // The arena was sized in run() for every worker's buckets.
    const auto reserved = click_slot(mapped, output.size());
    parallel(workers, workers, [&](size_t, size_t group) {
        const auto worker = worker_offset + group;
#ifdef __AVX2__
        struct WriteFence {
            ~WriteFence() { _mm_sfence(); }
        } fence_on_exit;
#endif
        if (reserved >= 64) {
            for (auto& bucket : output[worker]) {
#ifdef __AVX2__
                if (auto* memory = arena.allocate(reserved * sizeof(RawClick))) {
                    bucket.attach(static_cast<RawClick*>(memory), reserved);
                    continue;
                }
#endif
                bucket.reserve(reserved);
            }
        }
        std::vector<char> buffer;
        for (size_t part; (part = next.fetch_add(1, std::memory_order_relaxed)) < mapped.chunks.size();) {
            read_part<4>(mapped.load(part, buffer), [&](const auto& fields) {
                if (fields[1].empty() || fields[2].empty()) return;
                // Start both ID lookups before the timestamp work.
                const auto click_id = keys.prepare(fields[0]);
                const auto user_id = keys.prepare(fields[1]);
                auto ts = timestamp_ms(fields[3]);
                if (!ts || *ts < start - week_ms || *ts >= end) return;
                auto campaign = dims.campaigns.get(keys.encode(fields[2]), missing_campaign);
                if (campaign == missing_campaign) return;
                const auto user = keys.resolve(user_id);
                const auto user_hash = hash_key(user);
                const auto person = aliases.get(user, user_hash);
                const auto hash = person == user ? user_hash : hash_key(person);
                const auto offset = static_cast<uint32_t>(*ts - (start - week_ms));
                output[worker][bucket_from_hash<person_bucket_count>(hash)].push_back(
                    {hash, keys.resolve(click_id), offset, campaign, 0});
            });
        }
#ifdef __AVX2__
        for (auto& bucket : output[worker]) bucket.flush_tail();
#endif
    });
}

struct TimelineEvent {
    uint32_t ts, purchase;
};
struct TimelineClick {
    Key id;
    uint32_t ts, campaign;
};

class Attribution {
    std::vector<uint32_t> active, counts, position;
    size_t left = 0, right = 0;

public:
    explicit Attribution(size_t campaigns) : counts(campaigns), position(campaigns) {
        active.reserve(campaigns);
    }
    void reset() {
        for (auto campaign : active) counts[campaign] = 0;
        active.clear();
        left = right = 0;
    }
    void credit(std::span<const TimelineClick> clicks, int64_t ts, size_t base,
                int64_t revenue, std::vector<CampaignMetrics>& daily) {
        while (right < clicks.size() && clicks[right].ts <= ts) {
            auto campaign = clicks[right++].campaign;
            if (!counts[campaign]++) {
                position[campaign] = active.size();
                active.push_back(campaign);
            }
        }
        while (left < right && clicks[left].ts < ts - week_ms) {
            auto campaign = clicks[left++].campaign;
            if (!--counts[campaign]) {
                auto last = active.back();
                active[position[campaign]] = last;
                position[last] = position[campaign];
                active.pop_back();
            }
        }
        auto campaign = left < right ? clicks[right - 1].campaign : counts.size() - 1;
        auto& credited = daily[base + campaign];
        ++credited.sessions;
        credited.purchasing += revenue > 0;
        credited.revenue += revenue;
        for (auto assisted : active) ++daily[base + assisted].assisted;
    }
};

// Events within a bin are at most one session gap apart. The first
// and last timestamps suffice to decide whether consecutive bins join.
class TimeBins {
    static constexpr size_t capacity = UINT32_MAX / session_gap + 1;

    struct Bin {
        uint32_t first;
        uint32_t last;
        uint32_t count;
        uint32_t purchase_head;
    };
    struct Purchase {
        uint32_t value;
        uint32_t next;
    };

    std::array<Bin, capacity> bins;
    std::array<uint64_t, (capacity + 63) / 64> occupied{};
    std::vector<Purchase> purchases;
    static_assert((capacity + 63) / 64 <= 64);
    uint64_t used_words = 0;

public:
    void add(std::span<const TimelineEvent> events) {
        for (const auto& event : events) {
            const auto slot = event.ts / session_gap;
            auto& bin = bins[slot];
            const size_t word = slot / 64;
            const uint64_t bit = uint64_t{1} << (slot % 64);
            if (!(occupied[word] & bit)) {
                bin = {event.ts, event.ts, 1, UINT32_MAX};
                occupied[word] |= bit;
                used_words |= uint64_t{1} << word;
            } else {
                bin.first = std::min(bin.first, event.ts);
                bin.last = std::max(bin.last, event.ts);
                ++bin.count;
            }
            if (event.purchase) {
                purchases.push_back({event.purchase, bin.purchase_head});
                bin.purchase_head = static_cast<uint32_t>(purchases.size() - 1);
            }
        }
    }

    template<class Visit>
    void drain(Visit visit) {
        while (used_words) {
            const size_t word = std::countr_zero(used_words);
            for (auto mask = occupied[word]; mask; mask &= mask - 1) {
                visit(bins[word * 64 + std::countr_zero(mask)]);
            }
            occupied[word] = 0;
            used_words &= used_words - 1;
        }
        purchases.clear();
    }

    template<class Visit>
    void visit_purchases(uint32_t head, Visit visit) const {
        for (; head != UINT32_MAX; head = purchases[head].next) visit(purchases[head].value);
    }
};

// Per-worker scratch space reused across person buckets.
struct SummaryScratch {
    struct Person {
        uint64_t hash;
        uint32_t events = 0, clicks = 0;
        uint32_t event_cursor = 0, click_cursor = 0;
    };
    std::vector<Person> people;
    std::vector<uint32_t> event_groups, click_groups;
    std::vector<TimelineEvent> events;
    std::vector<TimelineClick> clicks;
    std::vector<int64_t> category_revenue;
    std::vector<uint32_t> purchased_categories;
    TimeBins bins;
};

void summarize_bucket(const std::vector<PersonEventRows>& event_feeds, const std::vector<RawClickRows>& click_feeds,
                      size_t bucket, const Keys& keys, const Dimensions& dims, int64_t start, int32_t first_day,
                      Attribution& attribution, parquet::File& users,
                      std::vector<CampaignMetrics>& campaign_days, SummaryScratch& scratch) {
    size_t event_count = 0, click_count = 0;
    for (const auto& feed : event_feeds) event_count += feed[bucket].size();
    for (const auto& feed : click_feeds) click_count += feed[bucket].size();
    if (!event_count) return;
    if (uint64_t{event_count} + click_count >= UINT32_MAX) throw WideEventLayout{};

    // Group the bucket's events and clicks by person with a counting scatter.
    auto& people = scratch.people;
    people.clear();
    Lookup<uint32_t, true> index(event_count / 8 + 1);
    auto group_of = [&](uint64_t hash) {
        const auto next = static_cast<uint32_t>(people.size());
        index.reserve(next + 1);
        const auto group = index.find_or_insert(hash, next);
        if (group == next) people.push_back({.hash = hash});
        return group;
    };
    auto& event_groups = scratch.event_groups;
    auto& click_groups = scratch.click_groups;
    event_groups.resize(event_count);
    click_groups.resize(click_count);
    size_t row = 0;
    for (const auto& feed : event_feeds) {
        for (const auto& event : feed[bucket]) {
            const auto group = group_of(event.person_hash);
            ++people[group].events;
            event_groups[row++] = group;
        }
    }
    row = 0;
    for (const auto& feed : click_feeds) {
        for (const auto& click : feed[bucket]) {
            // Clicks of people without events never credit a session.
            const auto group = index.get(click.person_hash, UINT32_MAX);
            if (group != UINT32_MAX) ++people[group].clicks;
            click_groups[row++] = group;
        }
    }
    uint32_t event_offset = 0, click_offset = 0;
    for (auto& person : people) {
        person.event_cursor = event_offset;
        person.click_cursor = click_offset;
        event_offset += person.events;
        click_offset += person.clicks;
    }
    if (scratch.events.size() < event_offset) scratch.events.resize(event_offset);
    if (scratch.clicks.size() < click_offset) scratch.clicks.resize(click_offset);
    auto* timeline = scratch.events.data();
    auto* click_timeline = scratch.clicks.data();
    row = 0;
    for (const auto& feed : event_feeds) {
        for (const auto& event : feed[bucket]) {
            timeline[people[event_groups[row++]].event_cursor++] = {event.ts, event.purchase};
        }
    }
    row = 0;
    for (const auto& feed : click_feeds) {
        for (const auto& click : feed[bucket]) {
            const auto group = click_groups[row++];
            if (group != UINT32_MAX) {
                click_timeline[people[group].click_cursor++] = {click.id, click.ts, click.campaign};
            }
        }
    }

    auto& category_revenue = scratch.category_revenue;
    category_revenue.assign(dims.categories.size(), 0);
    auto& purchased_categories = scratch.purchased_categories;
    auto& bins = scratch.bins;
    for (const auto& person : people) {
        const auto key = unhash_key(person.hash);
        // The cursors now point just past each person's slice.
        std::span<TimelineEvent> person_events(timeline + person.event_cursor - person.events, person.events);
        std::span<TimelineClick> person_clicks(click_timeline + person.click_cursor - person.clicks, person.clicks);
        constexpr size_t binning_threshold = 16;
        const bool use_bins = person.events >= binning_threshold;
        if (use_bins) {
            bins.add(person_events);
        } else {
            std::sort(person_events.begin(), person_events.end(), [](const auto& a, const auto& b) { return a.ts < b.ts; });
        }
        std::sort(person_clicks.begin(), person_clicks.end(), [&](const auto& a, const auto& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            return keys.less(b.id, a.id);
        });
        attribution.reset();
        UserDay daily{.person = key};
        auto emit_day = [&] {
            int64_t largest = 0;
            for (auto category : purchased_categories) {
                auto revenue = category_revenue[category];
                if (revenue > largest || (revenue == largest && dims.categories[category] < dims.categories[daily.category])) {
                    largest = revenue;
                    daily.category = category;
                }
                category_revenue[category] = 0;
            }
            purchased_categories.clear();
            parquet::append_user(users, daily, keys, dims.categories);
        };
        auto begin_session = [&](uint32_t session_start) {
            const auto day = utc_day(start + session_start);
            if (day != daily.day) {
                if (daily.sessions) emit_day();
                daily = UserDay{.person = key, .day = day};
            }
            ++daily.sessions;
        };
        auto add_purchase = [&](uint32_t purchase) {
            const auto category = purchase >> 24;
            const auto price = purchase & price_mask;
            if (!category_revenue[category]) purchased_categories.push_back(category);
            category_revenue[category] += price;
            return price;
        };
        auto finish_session = [&](uint32_t session_start, int64_t revenue) {
            daily.revenue += revenue;
            attribution.credit(person_clicks, session_start + week_ms,
                               static_cast<size_t>(daily.day - first_day) * dims.campaign_values.size(),
                               revenue, campaign_days);
        };
        if (use_bins) {
            uint32_t session_start = invalid_event, previous = 0;
            int64_t revenue = 0;
            bins.drain([&](const auto& bin) {
                if (session_start == invalid_event || bin.first - previous > session_gap) {
                    if (session_start != invalid_event) finish_session(session_start, revenue);
                    session_start = bin.first;
                    begin_session(session_start);
                    revenue = 0;
                }
                previous = bin.last;
                daily.events += bin.count;
                bins.visit_purchases(bin.purchase_head, [&](uint32_t purchase) {
                    revenue += add_purchase(purchase);
                });
            });
            finish_session(session_start, revenue);
        } else {
            for (size_t event = 0; event < person_events.size();) {
                const auto session_start = person_events[event].ts;
                begin_session(session_start);
                uint32_t previous;
                int64_t revenue = 0;
                do {
                    const auto& entry = person_events[event++];
                    previous = entry.ts;
                    ++daily.events;
                    if (entry.purchase) revenue += add_purchase(entry.purchase);
                } while (event < person_events.size() && person_events[event].ts - previous <= session_gap);
                finish_session(session_start, revenue);
            }
        }
        emit_day();
    }
}

int run(const fs::path& input, const fs::path& output, int64_t start, int64_t end, size_t workers) {
    auto last = std::chrono::steady_clock::now();
    const bool profile = std::getenv("SHOPPING_PROFILE");
    profile_workers = profile;
    long last_faults = 0;
    double last_system = 0;
    auto mark = [&](const char* stage) {
        auto now = std::chrono::steady_clock::now();
        if (profile) {
            struct rusage usage {};
            getrusage(RUSAGE_SELF, &usage);
            const double system = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
            std::cerr << stage << ": " << std::chrono::duration<double>(now - last).count() << " s, "
                      << usage.ru_minflt - last_faults << " faults, " << system - last_system << " s system\n";
            last_faults = usage.ru_minflt;
            last_system = system;
        }
        last = now;
    };
    // Long IDs are interned in a table sized from the input. About one row
    // in eight brings a new long ID, so a slot per four rows (rounded up to a
    // power of two) keeps the table under half full.
    size_t input_bytes = 0;
    for (const char* folder : {"events", "clicks", "links"}) {
        if (!fs::exists(input / folder)) continue;
        for (auto& entry : fs::directory_iterator(input / folder)) {
            if (entry.is_regular_file()) input_bytes += entry.file_size();
        }
    }
    Keys keys(input_bytes / 40 / 4 / 2);
    const auto dims = dimensions(keys, input);
    if (dims.categories.size() > 256) return 10;
    PersonAliases people;
    mark("dimensions");
    Arena arena, person_arena, click_arena;
    std::vector<RawClickRows> clicks(workers);
    std::vector<PersonEventRows> person_events(workers);
    {
        ChunkedInput event_input, click_input;
        event_input.folder(input / "events", read_chunk);
        click_input.folder(input / "clicks", read_chunk);
        // Event-ID buckets keep all copies of an event together, even if their user IDs differ.
        std::vector<EventBuckets> events(workers);
        std::atomic<size_t> next_click{0};
        const auto estimated_events = event_input.estimate_rows(27);
        if (const auto slot = click_slot(click_input, workers); slot >= 64) {
            click_arena.reserve(workers * person_bucket_count * slot * sizeof(RawClick));
        }
        if (workers == 1) {
            people = link_people(keys, input);
            read_events(event_input, 1, start, end, keys, dims, events, arena);
            read_clicks(click_input, 1, start, end, keys, dims, people, clicks, click_arena, next_click, 0);
        } else {
            // Clicks are routed by person, so the main workers wait for the
            // linked IDs before they parse their share of the clicks.
            std::exception_ptr link_or_click_error;
            std::atomic<bool> linked{false};
            std::jthread resolve([&] {
                try {
                    people = link_people(keys, input);
                    linked = true;
                    linked.notify_all();
                    read_clicks(click_input, 1, start, end, keys, dims, people, clicks, click_arena, next_click, 0);
                } catch (...) {
                    link_or_click_error = std::current_exception();
                    linked = true;
                    linked.notify_all();
                }
            });
            read_events(event_input, workers - 1, start, end, keys, dims, events, arena);
            linked.wait(false);
            if (!link_or_click_error) {
                read_clicks(click_input, workers - 1, start, end, keys, dims, people, clicks, click_arena, next_click, 1);
            }
            resolve.join();
            if (link_or_click_error) std::rethrow_exception(link_or_click_error);
        }
        mark("parse and link");
        // Person buckets hold the surviving events, about one per parsed row.
        const auto person_slot = (estimated_events / workers / person_bucket_count * 9 / 8 + 1) & ~size_t{1};
        if (person_slot >= 64) {
            person_arena.reserve(workers * person_bucket_count * person_slot * sizeof(PersonEvent));
            parallel(workers, workers, [&](size_t, size_t worker) {
                for (auto& bucket : person_events[worker]) {
#ifdef __AVX2__
                    if (auto* memory = person_arena.allocate(person_slot * sizeof(PersonEvent))) {
                        bucket.attach(static_cast<PersonEvent*>(memory), person_slot);
                        continue;
                    }
#endif
                    bucket.reserve(person_slot);
                }
            });
        }
        deduplicate(events, workers, people, person_events);
#ifdef __AVX2__
        // Each worker's buckets were filled by whichever tasks it ran.
        parallel(workers, workers, [&](size_t, size_t worker) {
            for (auto& bucket : person_events[worker]) bucket.flush_tail();
            _mm_sfence();
        });
#endif
        mark("event dedup and person routing");
    }
    auto first_day = utc_day(start);
    auto day_count = utc_day(end - 1) - first_day + 1;
    size_t campaign_slots = day_count * dims.campaign_values.size();
    const auto user_output = output / "user_daily";
    fs::create_directories(user_output);
    std::vector<std::vector<CampaignMetrics>> campaigns(workers, std::vector<CampaignMetrics>(campaign_slots));
    std::vector<Attribution> attribution(workers, Attribution(dims.campaign_values.size()));
    std::atomic<size_t> next_bucket{0};
    parallel(workers, workers, [&](size_t, size_t worker) {
        parquet::File writer(user_output / ("part-" + std::to_string(worker) + ".parquet"), parquet::user_schema);
        SummaryScratch scratch;
        for (size_t bucket; (bucket = next_bucket.fetch_add(1, std::memory_order_relaxed)) < person_bucket_count;) {
            summarize_bucket(person_events, clicks, bucket, keys, dims, start, first_day,
                             attribution[worker], writer, campaigns[worker], scratch);
        }
        writer.finish();
    });
    mark("group, sessionize, attribute and write user Parquet");
    for (size_t worker = 1; worker < workers; ++worker) {
        for (size_t slot = 0; slot < campaign_slots; ++slot) campaigns[0][slot] += campaigns[worker][slot];
    }
    write_campaigns(output, dims.campaign_values, campaigns[0], first_day);
    mark("campaign Parquet");
    // Process exit frees the remaining pages on one thread. Release the
    // large buffers in parallel first.
    parallel(workers, workers, [&](size_t, size_t worker) {
        arena.discard(worker, workers);
        person_arena.discard(worker, workers);
        click_arena.discard(worker, workers);
        for (auto& bucket : person_events[worker]) release_event_buffer(bucket);
        for (auto& bucket : clicks[worker]) release_event_buffer(bucket);
    });
    mark("release memory");
    if (profile) {
        struct rusage usage {};
        if (getrusage(RUSAGE_SELF, &usage) == 0) std::cerr << "peak RSS: " << usage.ru_maxrss << " KiB\n";
        std::ifstream status("/proc/self/status");
        for (std::string line; std::getline(status, line);) {
            if (line.starts_with("Rss")) std::cerr << line << '\n';
        }
    }
    // The writers have closed their files and joined. Let the kernel reclaim
    // the remaining in-memory result buffers in this one-shot executable.
    // A profiling build writes its profile through an exit handler. The check
    // is at run time so training and release builds share control flow.
    if (std::getenv("SHOPPING_PGO_TRAINING")) std::exit(0);
    if (sampler_dump) sampler_dump();
    std::_Exit(0);
}
} // namespace shopping

namespace {
std::optional<std::array<std::string, 2>> manifest_window(const shopping::fs::path& data) {
    std::ifstream file(data / "manifest.json");
    std::string json{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    std::string_view rest = json;
    std::array<std::string, 2> values;
    auto trim = [&] {
        const auto first = rest.find_first_not_of(" \r\n\t");
        rest.remove_prefix(first == std::string_view::npos ? rest.size() : first);
    };
    auto take = [&](char token) {
        trim();
        if (rest.empty() || rest.front() != token) return false;
        rest.remove_prefix(1);
        return true;
    };
    auto quoted = [&]() -> std::optional<std::string_view> {
        if (!take('"')) return std::nullopt;
        const auto end = rest.find_first_of("\"\\");
        if (end == std::string_view::npos || rest[end] != '"') return std::nullopt;
        auto value = rest.substr(0, end);
        rest.remove_prefix(end + 1);
        return value;
    };
    // Extended JSON (object members or escaped strings) uses the reference
    // launcher, which already has a complete JSON parser.
    if (!take('{')) return std::nullopt;
    for (int i = 0; i < 2; ++i) {
        auto key = quoted();
        if (!key || !take(':')) return std::nullopt;
        auto value = quoted();
        if (!value || value->empty()) return std::nullopt;
        if (*key == "window_start") values[0] = *value;
        else if (*key == "window_end") values[1] = *value;
        else return std::nullopt;
        if (!take(i == 0 ? ',' : '}')) return std::nullopt;
    }
    trim();
    if (!rest.empty() || values[0].empty() || values[1].empty()) return std::nullopt;
    return values;
}

const char* environment(const char* name, const char* fallback) {
    if (auto value = std::getenv(name); value && *value) return value;
    return fallback;
}

int invoke(const shopping::fs::path& data, const shopping::fs::path& output,
           std::string_view begin, std::string_view end, const char* threads) {
    auto start_ms = timestamp_ms(begin);
    auto end_ms = timestamp_ms(end);
    if (!start_ms || !end_ms || *end_ms <= *start_ms) throw std::runtime_error("invalid manifest window");
    if (static_cast<uint64_t>(*end_ms) - static_cast<uint64_t>(*start_ms) > shopping::invalid_event - shopping::week_ms) return 10;
    auto workers = std::stoul(threads);
    if (!workers) throw std::runtime_error("thread count must be positive");
    return shopping::run(data, output, *start_ms, *end_ms, workers);
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 1) {
            const auto* data = environment("DATA_DIR", "/data");
            const auto* threads = environment("NCPU", nullptr);
            if (!threads) return 10;
            auto window = manifest_window(data);
            if (!window) return 10;
            return invoke(data, environment("OUTPUT_DIR", "/output"),
                          (*window)[0], (*window)[1], threads);
        }
        if (argc == 6) return invoke(argv[1], argv[2], argv[3], argv[4], argv[5]);
        throw std::runtime_error("usage: native_pipeline [DATA OUTPUT START END THREADS]");
    } catch (const shopping::WideEventLayout&) {
        return 10;
    } catch (const shopping::InputLayoutLimit&) {
        return 10;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
