/*
 * Question 2
 * Given 2 UDP streams, A and B, we would like to implement a consumer who
 *   receives all messages in order of sequence number.
 * Streams A and B are redundant, meaning both streams will be sent all of the
 *   same messages.
 * Please implement the below class, adding any members or helper functions
 *   necessary.
 * The passMessageToConsumer method should be called by onMessage every time
 *   it has the next message available.
 * For simplification, you can assume you will always receive at least one copy
 *   of each message
 * You should focus on performance
 * You may use the std libraries or STL, as well as any features through C++20
 */

/*
 * ---------------------------------------------------------------------------
 * DESIGN  (classic A/B "line arbitration" as used on exchange feeds)
 * ---------------------------------------------------------------------------
 *  State: `nextExpected_` = the sequence number the consumer needs next.
 *
 *  For every incoming packet (from either stream) there are exactly 3 cases:
 *
 *   seq <  nextExpected_  DUPLICATE. Already delivered (the other line won the
 *                         race). One compare, drop.               -> O(1)
 *
 *   seq == nextExpected_  IN-ORDER. Deliver straight out of the caller's
 *                         buffer (ZERO COPY), bump nextExpected_, then drain
 *                         any buffered messages that are now contiguous.
 *                         This is the hot path.                   -> O(1)
 *
 *   seq >  nextExpected_  GAP. Something earlier is still missing on this line.
 *                         The caller's buffer dies when onMessage returns, so
 *                         we must copy the payload into a reorder buffer and
 *                         wait for the missing messages (from the other line).
 *
 *  REORDER BUFFER = ring of slots indexed by (seq & mask):
 *   - O(1) insert / lookup / erase, no hashing, no tree nodes, no per-message
 *     heap allocation in steady state (slots keep their payload capacity).
 *   - Contiguous, cache-friendly memory (vs std::map / unordered_map nodes).
 *   - A copy of a buffered message arriving from the *other* stream is
 *     recognised by `occupied` and ignored -> also deduplicates buffered msgs.
 *
 *   Window invariant: every buffered seq satisfies
 *        nextExpected_ < seq < nextExpected_ + capacity
 *   so two live messages can never map to the same slot.
 *   If a message arrives further ahead than the window, the ring is grown
 *   (cold path, doubles to the next power of two, capped at kMaxCapacity).
 *
 * ASSUMPTIONS
 *   - Sequence numbers start at 1 (constructor arg lets you change that) and
 *     are contiguous, i.e. no holes: every seq is eventually seen at least once
 *     (as stated in the question).
 *   - onMessage() is invoked from ONE thread (typical: a single busy-polling
 *     thread reads both sockets). No internal locking, by design. If A and B
 *     were read on different threads they must be serialised by the caller
 *     (or funnelled through one thread / an SPSC queue) - a lock here would
 *     cost more than the arbitration itself.
 *   - `stream` is deliberately not used: the two lines are redundant, so the
 *     first copy to arrive wins regardless of which line it came from.
 *   - A seq that is absurdly far ahead (>= kMaxCapacity) is treated as
 *     corrupt: it is dropped and counted (droppedOutOfWindow()). In production
 *     that would trigger a gap-fill / snapshot request instead.
 * ---------------------------------------------------------------------------
 */
#include <bit>          // std::bit_ceil
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <utility>      // std::move
#include <vector>

class StreamHandler {
    // ---- Delivery to the consumer -----------------------------------------
    // Takes a raw pointer + length (not std::string / std::vector) so that the
    // in-order fast path can hand over the packet buffer without copying.
    // The pointer is only valid for the duration of the call.
    void passMessageToConsumer(const std::uint64_t seqnum,
                               const char* bytes,
                               const std::size_t byteCount) {
        // '\n' rather than std::endl: no flush per message.
        std::cout << "Consumer received message: seq=" << seqnum << " payload=\"";
        std::cout.write(bytes, static_cast<std::streamsize>(byteCount));
        std::cout << "\"\n";
    }

public:
    // firstSeq       : first sequence number the consumer expects (1 here).
    // initialCapacity: reorder window in messages; rounded up to a power of 2
    //                  so that (seq & mask) replaces a modulo.
    // payloadHint    : bytes pre-reserved per slot so that buffering a typical
    //                  message does not touch the allocator on the hot path.
    explicit StreamHandler(const std::uint64_t firstSeq = 1,
                           const std::size_t initialCapacity = 1024,
                           const std::size_t payloadHint = 256)
        : nextExpected_(firstSeq),
          slots_(std::bit_ceil(initialCapacity < 2 ? std::size_t{2} : initialCapacity)),
          mask_(slots_.size() - 1) {
        for (Slot& s : slots_) s.payload.reserve(payloadHint);
    }

    void onMessage(const char stream, const uint64_t seqnum,
                   const char* bytes, const std::size_t byteCount)
    {
        (void)stream;  // redundant lines: arbitration is stream-agnostic

        // 1) IN-ORDER (hot path): deliver directly, no copy, then flush any
        //    buffered successors that just became contiguous.
        if (seqnum == nextExpected_) {
            passMessageToConsumer(seqnum, bytes, byteCount);
            ++nextExpected_;
            drainBuffered();
            return;
        }

        // 2) DUPLICATE: we already delivered this seq (other line was faster).
        if (seqnum < nextExpected_) {
            return;
        }

        // 3) GAP: a future message. Copy it aside until its predecessors show up.
        bufferOutOfOrder(seqnum, bytes, byteCount);
    }

    // ---- Introspection (handy for tests / monitoring) ----------------------
    std::uint64_t nextExpected()        const { return nextExpected_; }
    std::size_t   bufferedCount()       const { return buffered_; }
    std::uint64_t droppedOutOfWindow()  const { return dropped_; }

private:
    struct Slot {
        std::uint64_t     seq      = 0;
        bool              occupied = false;
        std::vector<char> payload;   // capacity is retained after use -> reused
    };

    // Upper bound for the reorder window (1M slots). Protects against a
    // corrupt/hostile seqnum making us allocate gigabytes.
    static constexpr std::size_t kMaxCapacity = std::size_t{1} << 20;

    // Delivers every consecutive buffered message starting at nextExpected_.
    // Stops at the first hole (that message is still missing on both lines).
    void drainBuffered() {
        // `buffered_ == 0` is the common case: skip touching the ring at all.
        while (buffered_ != 0) {
            Slot& s = slots_[nextExpected_ & mask_];
            if (!s.occupied) break;                 // hole -> wait for more input
            assert(s.seq == nextExpected_);         // guaranteed by window invariant

            passMessageToConsumer(s.seq, s.payload.data(), s.payload.size());
            s.occupied = false;                     // slot is free again;
            --buffered_;                            // payload capacity is kept
            ++nextExpected_;
        }
    }

    // Cold-ish path: stores a message that arrived ahead of its turn.
    void bufferOutOfOrder(const std::uint64_t seq, const char* bytes,
                          const std::size_t byteCount) {
        const std::uint64_t distance = seq - nextExpected_;   // >= 1 here

        if (distance >= slots_.size()) [[unlikely]] {
            // Doesn't fit in the current window: grow, or drop if absurd.
            if (distance >= kMaxCapacity || !growRing(distance + 1)) {
                ++dropped_;
                return;
            }
        }

        Slot& s = slots_[seq & mask_];
        if (s.occupied) return;   // the other line already delivered us this one

        s.seq      = seq;
        s.occupied = true;
        s.payload.assign(bytes, bytes + byteCount);   // reuses capacity: no malloc
        ++buffered_;
    }

    // Grows the ring to hold at least `needed` slots and re-homes live entries
    // (their index depends on the mask). Happens only on large reordering.
    bool growRing(const std::size_t needed) {
        if (needed > kMaxCapacity) return false;
        const std::size_t newCapacity = std::bit_ceil(needed);
        const std::size_t newMask     = newCapacity - 1;

        std::vector<Slot> bigger(newCapacity);
        for (Slot& s : slots_) {
            if (s.occupied) bigger[s.seq & newMask] = std::move(s);
        }
        slots_ = std::move(bigger);
        mask_  = newMask;
        return true;
    }

    std::uint64_t     nextExpected_;      // next seq the consumer must receive
    std::vector<Slot> slots_;             // reorder ring, size = power of two
    std::size_t       mask_;              // slots_.size() - 1
    std::size_t       buffered_ = 0;      // number of occupied slots
    std::uint64_t     dropped_  = 0;      // seqs rejected as out-of-window
};

int main() {
    StreamHandler handler;
    handler.onMessage('A', 1, "Msg 1", 5);
    handler.onMessage('B', 1, "Msg 1", 5);
    handler.onMessage('B', 2, "Msg 2", 5);
    handler.onMessage('A', 2, "Msg 2", 5);
    handler.onMessage('B', 5, "Msg 5", 5);
    handler.onMessage('A', 4, "Msg 4", 5);
    handler.onMessage('A', 3, "Msg 3", 5);
    handler.onMessage('A', 5, "Msg 5", 5);
    handler.onMessage('B', 3, "Msg 3", 5);
    handler.onMessage('B', 4, "Msg 4", 5);

    return 0;
}
