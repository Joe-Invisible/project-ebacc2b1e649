#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <algorithm>
#include <cstdint>
#include <mutex>

namespace duckdb {
namespace {

// The pipeline supplies non-null timestamps in ascending order within each person.
class SessionStartFunction : public WindowFunction {
    struct Boundary {
        idx_t row;
        int64_t timestamp;
    };

    struct State : WindowExecutorGlobalState {
        using WindowExecutorGlobalState::WindowExecutorGlobalState;
        std::once_flag initialized;
        vector<Boundary> boundaries;
    };

    static void Share(WindowExecutor &executor, WindowSharedExpressions &shared) {
        executor.child_idx.push_back(shared.RegisterCollection(executor.wexpr.GetChildren()[0], false));
    }

    static unique_ptr<GlobalSinkState> Global(ClientContext &client, const WindowExecutor &executor,
                                             idx_t count, const ValidityMask &partitions, const ValidityMask &order) {
        return make_uniq<State>(client, executor, count, partitions, order);
    }

    static void Finalize(ExecutionContext &, optional_ptr<WindowCollection> collection, OperatorSinkInput &sink) {
        auto &state = sink.global_state.Cast<State>();
        std::call_once(state.initialized, [&] {
            constexpr int64_t gap_ms = 1800000;
            WindowCursor cursor(*collection, state.executor.child_idx[0]);
            int64_t previous = 0;
            idx_t row = 0;
            while (cursor.Scan()) {
                const auto timestamps = FlatVector::GetData<const int64_t>(cursor.chunk.data[0]);
                for (idx_t i = 0; i < cursor.chunk.size(); ++i, ++row) {
                    const auto timestamp = timestamps[i];
                    const bool gap = timestamp > previous &&
                        static_cast<uint64_t>(timestamp) - static_cast<uint64_t>(previous) > gap_ms;
                    if (row == 0 || state.partition_mask.RowIsValid(row) || gap) {
                        state.boundaries.push_back({row, timestamp});
                    }
                    previous = timestamp;
                }
            }
        });
    }

    static void Evaluate(ExecutionContext &, DataChunk &input, DataChunk &, Vector &result,
                         idx_t row, OperatorSinkInput &sink) {
        const auto &boundaries = sink.global_state.Cast<State>().boundaries;
        auto next = std::upper_bound(boundaries.begin(), boundaries.end(), row,
                                     [](idx_t index, const Boundary &boundary) { return index < boundary.row; });
        auto start = (next - 1)->timestamp;
        result.SetVectorType(VectorType::FLAT_VECTOR);
        auto output = FlatVector::GetDataMutable<int64_t>(result);
        for (idx_t i = 0; i < input.size(); ++i, ++row) {
            if (next != boundaries.end() && row == next->row) {
                start = next++->timestamp;
            }
            output[i] = start;
        }
    }

public:
    SessionStartFunction()
        : WindowFunction("shopping_session_start", {LogicalType::BIGINT}, LogicalType::BIGINT,
                         ExpressionType::WINDOW_FUNCTION, nullptr, nullptr, Share, Global,
                         nullptr, nullptr, Finalize, Evaluate) {
        SetCanOrderBy(false);
        SetCanIgnoreNulls(false);
    }
};

} // namespace

extern "C" DUCKDB_CPP_EXTENSION_ENTRY(shopping_window, loader) {
    loader.RegisterFunction(SessionStartFunction());
}
} // namespace duckdb
