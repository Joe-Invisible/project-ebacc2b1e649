#include "native_input.hpp"
#include "timestamp.hpp"
#include "native_output.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <future>
#include <limits>
#include <span>
#include <sys/resource.h>

namespace shopping {
constexpr int64_t day_ms = 86400000;
constexpr int64_t week_ms = 7 * day_ms;
constexpr uint32_t session_gap = 1800000;
constexpr size_t bucket_count = 128;
constexpr size_t input_chunk = 2 * 1024 * 1024;
constexpr uint32_t invalid_event = std::numeric_limits<uint32_t>::max();
constexpr uint32_t missing_campaign = std::numeric_limits<uint32_t>::max();
constexpr int64_t ingest_bias = 62135596800; // Seconds between year 1 UTC and Unix epoch.
constexpr uint64_t price_mask = (uint64_t{1} << 24) - 1;

int32_t utc_day(int64_t value) {
    return value / day_ms - (value % day_ms < 0);
}

struct Event {
    Key id;
    Key user;
    uint64_t ingest_and_price;
    uint32_t ts;
    uint32_t category;
};
struct SessionEvent {
    Key person;
    uint32_t ts;
    uint32_t purchase; // Low 24 bits are price in cents, high 8 bits are category.
};
struct Click {
    Key person;
    Key id;
    uint32_t ts; // Offset from the beginning of the click window.
    uint32_t campaign;
};

template<class T>
using Buckets = std::array<std::vector<T>, bucket_count>;

size_t bucket_from_hash(uint64_t hash) {
    static_assert(std::has_single_bit(bucket_count));
    return hash >> (64 - std::bit_width(bucket_count - 1));
}

size_t bucket_for(Key key) {
    return bucket_from_hash(hash_key(key));
}

Lookup<Key> link_people(Keys& keys, const fs::path& input, size_t workers) {
    MappedInput links;
    links.folder(input / "links", 256 * 1024);
    struct Edge { Key first, second; };
    struct IndexedEdge { uint32_t first, second; };
    std::vector<std::vector<Edge>> edges(links.parts.size());
    parallel(workers, links.parts.size(), [&](size_t, size_t part) {
        read_part<2>(links.parts[part], [&](const auto& fields) {
            if (!fields[1].empty()) edges[part].push_back({keys.encode(fields[0]), keys.encode(fields[1])});
        });
    });
    size_t count = 0;
    for (const auto& part : edges) count += part.size();
    const size_t capacity = std::bit_ceil(std::max<size_t>(2, count * 4));
    if (capacity > std::numeric_limits<uint32_t>::max()) throw std::runtime_error("too many linked IDs");
    std::vector<std::atomic<Key>> nodes(capacity);
    std::vector<std::atomic<uint32_t>> parents(capacity);
    auto slot_for = [&](Key key) {
        auto slot = static_cast<uint32_t>(hash_key(key) & (capacity - 1));
        for (;;) {
            auto other = nodes[slot].load(std::memory_order_relaxed);
            if (!other && nodes[slot].compare_exchange_strong(other, key, std::memory_order_relaxed)) {
                parents[slot].store(slot, std::memory_order_relaxed);
                return slot;
            }
            if (other == key) return slot;
            slot = (slot + 1) & (capacity - 1);
        }
    };
    std::vector<std::vector<IndexedEdge>> indexed(edges.size());
    parallel(workers, edges.size(), [&](size_t, size_t part) {
        auto& destination = indexed[part];
        destination.reserve(edges[part].size());
        for (auto [first, second] : edges[part]) destination.push_back({slot_for(first), slot_for(second)});
        std::vector<Edge>().swap(edges[part]);
    });
    auto root = [&](uint32_t node) {
        for (;;) {
            auto parent = parents[node].load(std::memory_order_relaxed);
            if (node == parent) return node;
            auto ancestor = parents[parent].load(std::memory_order_relaxed);
            parents[node].compare_exchange_weak(parent, ancestor, std::memory_order_relaxed);
            node = ancestor;
        }
    };
    // Parent links always point to a smaller ID, so concurrent unions cannot
    // create a cycle and the surviving root is the byte-wise minimum.
    parallel(workers, indexed.size(), [&](size_t, size_t part) {
        for (auto [first, second] : indexed[part]) {
            for (;;) {
                auto a = root(first), b = root(second);
                if (a == b) break;
                if (keys.less(nodes[b].load(std::memory_order_relaxed), nodes[a].load(std::memory_order_relaxed))) {
                    std::swap(a, b);
                }
                auto expected = b;
                if (parents[b].compare_exchange_weak(expected, a, std::memory_order_relaxed)) break;
            }
        }
    });

    Lookup<Key> people(count);
    constexpr size_t span = 64 * 1024;
    parallel(workers, (capacity + span - 1) / span, [&](size_t, size_t part) {
        for (size_t slot = part * span; slot < std::min(capacity, (part + 1) * span); ++slot) {
            auto key = nodes[slot].load(std::memory_order_relaxed);
            if (!key) continue;
            auto person = nodes[root(slot)].load(std::memory_order_relaxed);
            if (key != person) people.insert_distinct_concurrently(key, person);
        }
    });
    people.index_homes();
    return people;
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

void read_events(const MappedInput& mapped, size_t workers, int64_t start, int64_t end,
                 Keys& keys, const Dimensions& dims, std::vector<Buckets<Event>>& output) {
    parallel(workers, mapped.parts.size(), [&](size_t worker, size_t part) {
        read_part<7>(mapped.parts[part], [&](const auto& fields) {
            auto id = keys.encode(fields[0]);
            if (fields[3].size() != 20) throw std::runtime_error("invalid event ingest_ts");
            auto ingest = ingest_seconds(fields[3]);
            Event event{id, 0, static_cast<uint64_t>(ingest + ingest_bias) << 24, invalid_event, 0};
            auto ts = timestamp_ms(fields[2]);
            if (ts && *ts >= start && *ts < end && ingest * 1000 - *ts <= day_ms &&
                !fields[1].empty() && !fields[5].empty()) {
                if (fields[4] == "purchase") {
                    auto price = parse_price(fields[6]);
                    if (price) {
                        event.ts = *ts - start;
                        event.ingest_and_price |= price;
                        event.category = dims.products.get(keys.encode(fields[5]), 0);
                    }
                } else if (fields[4] == "view" || fields[4] == "cart") {
                    event.ts = *ts - start;
                }
                if (event.ts != invalid_event) event.user = keys.encode(fields[1]);
            }
            // Preserve invalid copies here so the earliest copy always wins.
            output[worker][bucket_for(id)].push_back(event);
        });
    });
}

void deduplicate(std::vector<Buckets<Event>>& events, size_t workers, const Lookup<Key>& people,
                 std::vector<Buckets<SessionEvent>>& output) {
    parallel(workers, bucket_count, [&](size_t worker, size_t bucket) {
        size_t size = 0;
        for (auto& feed : events) size += feed[bucket].size();
        if (!size) return;
        // An event in a singleton hash slot has no duplicate. Colliding slots
        // are resolved by exact ID, including invalid copies.
        const size_t homes = std::bit_ceil(std::max<size_t>(64, size * 8));
        std::vector<uint64_t> seen(homes / 64), repeated(homes / 64);
        size_t collisions = 0;
        for (const auto& feed : events) {
            for (const auto& event : feed[bucket]) {
                size_t home = hash_key(event.id) & (homes - 1);
                uint64_t bit = uint64_t{1} << (home % 64);
                auto old = seen[home / 64];
                seen[home / 64] = old | bit;
                if (old & bit) {
                    repeated[home / 64] |= bit;
                    ++collisions;
                }
            }
        }
        const size_t capacity = std::bit_ceil(std::max<size_t>(2, collisions * 4));
        std::vector<uint32_t> index(capacity, invalid_event);
        std::vector<const Event*> winners;
        winners.reserve(collisions * 2);
        auto emit = [&](const Event& event) {
            if (event.ts == invalid_event) return;
            auto hash = hash_key(event.user);
            auto person = people.get_filtered(event.user, hash, event.user);
            output[worker][bucket_from_hash(person == event.user ? hash : hash_key(person))].push_back({
                person, event.ts, static_cast<uint32_t>(event.ingest_and_price & price_mask) | (event.category << 24),
            });
        };
        for (const auto& feed : events) {
            for (const auto& event : feed[bucket]) {
                auto hash = hash_key(event.id);
                size_t home = hash & (homes - 1);
                if (!(repeated[home / 64] & (uint64_t{1} << (home % 64)))) {
                    emit(event);
                    continue;
                }
                size_t slot = hash & (capacity - 1);
                while (index[slot] != invalid_event && winners[index[slot]]->id != event.id) {
                    slot = (slot + 1) & (capacity - 1);
                }
                if (index[slot] == invalid_event) {
                    index[slot] = winners.size();
                    winners.push_back(&event);
                } else {
                    auto& current = winners[index[slot]];
                    if ((event.ingest_and_price >> 24) < (current->ingest_and_price >> 24)) current = &event;
                }
            }
        }
        for (const auto* event : winners) emit(*event);
        for (auto& feed : events) std::vector<Event>().swap(feed[bucket]);
    });
}

void read_clicks(const MappedInput& mapped, size_t workers, int64_t start, int64_t end, Keys& keys,
                 const Dimensions& dims, const Lookup<Key>& people, std::vector<Buckets<Click>>& output) {
    parallel(workers, mapped.parts.size(), [&](size_t worker, size_t part) {
        read_part<4>(mapped.parts[part], [&](const auto& fields) {
            if (fields[1].empty() || fields[2].empty()) return;
            auto ts = timestamp_ms(fields[3]);
            if (!ts || *ts < start - week_ms || *ts >= end) return;
            auto campaign = dims.campaigns.get(keys.encode(fields[2]), missing_campaign);
            if (campaign == missing_campaign) return;
            auto user = keys.encode(fields[1]);
            auto hash = hash_key(user);
            auto person = people.get_filtered(user, hash, user);
            output[worker][bucket_from_hash(person == user ? hash : hash_key(person))].push_back({person, keys.encode(fields[0]), static_cast<uint32_t>(*ts - (start - week_ms)), campaign});
        });
    });
}

struct TimelineEvent {
    uint32_t ts, purchase;
};
struct TimelineClick {
    Key id;
    uint32_t ts, campaign;
};
struct PersonRange {
    Key person;
    size_t events = 0, clicks = 0;
    size_t event_offset = 0, click_offset = 0;
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

void summarize_bucket(std::vector<Buckets<SessionEvent>>& event_sources,
                      std::vector<Buckets<Click>>& click_sources, size_t bucket,
                      const Keys& keys, const Dimensions& dims, int64_t start, int32_t first_day,
                      Attribution& attribution, std::vector<UserDay>& users,
                      std::vector<CampaignMetrics>& campaign_days) {
    size_t event_count = 0, click_count = 0;
    for (const auto& source : event_sources) event_count += source[bucket].size();
    for (const auto& source : click_sources) click_count += source[bucket].size();
    if (!event_count) {
        for (auto& source : click_sources) std::vector<Click>().swap(source[bucket]);
        return;
    }
    // Count each person's rows first, then scatter small timeline records
    // into contiguous spans. Only timestamps inside one person need sorting.
    Lookup<uint32_t> people(event_count / 16 + 1);
    std::vector<PersonRange> ranges;
    std::vector<uint32_t> event_groups, click_groups;
    event_groups.reserve(event_count);
    click_groups.reserve(click_count);
    for (const auto& source : event_sources) {
        for (const auto& event : source[bucket]) {
            auto next = static_cast<uint32_t>(ranges.size());
            people.reserve(next + 1);
            auto group = people.find_or_insert(event.person, next);
            if (group == next) ranges.push_back(PersonRange{.person = event.person});
            ++ranges[group].events;
            event_groups.push_back(group);
        }
    }
    for (const auto& source : click_sources) {
        for (const auto& click : source[bucket]) {
            auto group = people.get(click.person, missing_campaign);
            if (group != missing_campaign) ++ranges[group].clicks;
            click_groups.push_back(group);
        }
    }

    size_t event_offset = 0, click_offset = 0;
    for (auto& range : ranges) {
        range.event_offset = event_offset;
        range.click_offset = click_offset;
        event_offset += range.events;
        click_offset += range.clicks;
    }
    std::vector<TimelineEvent> events(event_offset);
    std::vector<TimelineClick> clicks(click_offset);
    size_t row = 0;
    for (auto& source : event_sources) {
        for (const auto& event : source[bucket]) {
            auto& range = ranges[event_groups[row++]];
            events[range.event_offset++] = {event.ts, event.purchase};
        }
        std::vector<SessionEvent>().swap(source[bucket]);
    }
    row = 0;
    for (auto& source : click_sources) {
        for (const auto& click : source[bucket]) {
            auto group = click_groups[row++];
            if (group != missing_campaign) {
                clicks[ranges[group].click_offset++] = {click.id, click.ts, click.campaign};
            }
        }
        std::vector<Click>().swap(source[bucket]);
    }

    std::vector<int64_t> category_revenue(dims.categories.size());
    std::vector<uint32_t> purchased_categories;
    for (const auto& range : ranges) {
        auto person_events = std::span(events).subspan(range.event_offset - range.events, range.events);
        auto person_clicks = std::span(clicks).subspan(range.click_offset - range.clicks, range.clicks);
        std::sort(person_events.begin(), person_events.end(), [](const auto& a, const auto& b) { return a.ts < b.ts; });
        std::sort(person_clicks.begin(), person_clicks.end(), [&](const auto& a, const auto& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            return keys.less(b.id, a.id);
        });
        attribution.reset();
        UserDay daily{.person = range.person, .day = utc_day(start + person_events[0].ts)};
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
            users.push_back(daily);
        };
        for (size_t event = 0; event < person_events.size();) {
            auto session_start = person_events[event].ts;
            auto day = utc_day(start + session_start);
            if (day != daily.day) {
                emit_day();
                daily = UserDay{.person = range.person, .day = day};
            }
            ++daily.sessions;
            uint32_t previous = session_start;
            int64_t revenue = 0;
            do {
                const auto& row = person_events[event++];
                previous = row.ts;
                ++daily.events;
                if (row.purchase) {
                    auto category = row.purchase >> 24;
                    auto price = row.purchase & price_mask;
                    revenue += price;
                    if (!category_revenue[category]) purchased_categories.push_back(category);
                    category_revenue[category] += price;
                }
            } while (event < person_events.size() && person_events[event].ts - previous <= session_gap);
            daily.revenue += revenue;
            attribution.credit(person_clicks, session_start + week_ms,
                               static_cast<size_t>(day - first_day) * dims.campaign_values.size(), revenue, campaign_days);
        }
        emit_day();
    }
}

int run(const fs::path& input, const fs::path& output, int64_t start, int64_t end, size_t workers) {
    auto last = std::chrono::steady_clock::now();
    const bool profile = std::getenv("SHOPPING_PROFILE");
    auto mark = [&](const char* stage) {
        auto now = std::chrono::steady_clock::now();
        if (profile) std::cerr << stage << ": " << std::chrono::duration<double>(now - last).count() << " s\n";
        last = now;
    };
    Keys keys;
    const auto dims = dimensions(keys, input);
    if (dims.categories.size() > 256) return 10;
    Lookup<Key> people;
    mark("dimensions");
    // Event-ID buckets keep all revisions together, even if their user IDs differ.
    std::vector<Buckets<SessionEvent>> valid(workers);
    {
        MappedInput mapped;
        mapped.folder(input / "events", input_chunk);
        std::vector<Buckets<Event>> events(workers);
        if (workers == 1) {
            people = link_people(keys, input, 1);
            read_events(mapped, workers, start, end, keys, dims, events);
        } else {
            const auto graph_workers = std::min<size_t>(2, workers - 1);
            std::promise<Lookup<Key>> graph;
            auto result = graph.get_future();
            double link_seconds = 0;
            std::jthread resolve([&] {
                try {
                    auto begin = std::chrono::steady_clock::now();
                    auto resolved = link_people(keys, input, graph_workers);
                    link_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
                    graph.set_value(std::move(resolved));
                } catch (...) {
                    graph.set_exception(std::current_exception());
                }
            });
            auto parse_start = std::chrono::steady_clock::now();
            read_events(mapped, workers - graph_workers, start, end, keys, dims, events);
            auto parse_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - parse_start).count();
            people = result.get();
            if (profile) {
                std::cerr << "link graph: " << link_seconds << " s\n"
                          << "parse workers: " << parse_seconds << " s\n";
            }
        }
        mark("event parse and links");
        deduplicate(events, workers, people, valid);
        mark("event dedup");
    }

    // The second partitioning groups surviving events and clicks by canonical person.
    std::vector<Buckets<Click>> clicks(workers);
    {
        MappedInput mapped;
        mapped.folder(input / "clicks", input_chunk);
        read_clicks(mapped, workers, start, end, keys, dims, people, clicks);
    }
    mark("click parse");
    auto first_day = utc_day(start);
    auto day_count = utc_day(end - 1) - first_day + 1;
    size_t campaign_slots = day_count * dims.campaign_values.size();
    std::vector<std::vector<UserDay>> users(bucket_count);
    std::vector<std::vector<CampaignMetrics>> campaigns(workers, std::vector<CampaignMetrics>(campaign_slots));
    std::vector<Attribution> attribution(workers, Attribution(dims.campaign_values.size()));
    parallel(workers, bucket_count, [&](size_t worker, size_t bucket) {
        summarize_bucket(valid, clicks, bucket, keys, dims, start, first_day,
                         attribution[worker], users[bucket], campaigns[worker]);
    });
    mark("sort, sessionize and attribute");
    for (size_t worker = 1; worker < workers; ++worker) {
        for (size_t slot = 0; slot < campaign_slots; ++slot) campaigns[0][slot] += campaigns[worker][slot];
    }
    write_outputs(output, keys, dims.categories, dims.campaign_values, users, campaigns[0], first_day, workers);
    mark("Parquet output");
    if (profile) {
        struct rusage usage {};
        if (getrusage(RUSAGE_SELF, &usage) == 0) std::cerr << "peak RSS: " << usage.ru_maxrss << " KiB\n";
    }
    return 0;
}
} // namespace shopping

int main(int argc, char** argv) {
    try {
        if (argc != 6) throw std::runtime_error("usage: native_pipeline DATA OUTPUT START END THREADS");
        auto start = timestamp_ms(argv[3]);
        auto end = timestamp_ms(argv[4]);
        if (!start || !end || *end <= *start) throw std::runtime_error("invalid manifest window");
        if (static_cast<uint64_t>(*end) - static_cast<uint64_t>(*start) > shopping::invalid_event - shopping::week_ms) return 10;
        auto workers = std::stoul(argv[5]);
        if (!workers) throw std::runtime_error("thread count must be positive");
        return shopping::run(argv[1], argv[2], *start, *end, workers);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
