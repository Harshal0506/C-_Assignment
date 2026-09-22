/*
 * Question 3
 * Implement thread safe high performant queue (Single Producer, Multiple consumer) to hold quotes.
 *
 * The primary objective is to showcase your understanding of thread-safety.
 * 
 */

/*
 * ---------------------------------------------------------------------------
 * DESIGN OVERVIEW
 * ---------------------------------------------------------------------------
 *  Bounded, lock-free ring buffer (Vyukov-style, specialised for ONE producer)
 *  with a spin-then-park blocking layer on top.
 *
 *  Semantics: a WORK QUEUE. Every quote is delivered to exactly ONE consumer
 *  (not broadcast), in FIFO order of claiming. Nothing is lost or duplicated.
 *
 *  1. Ring of cache-line-sized slots, capacity = power of two (index = pos & mask).
 *
 *  2. Each slot has an atomic sequence number `seq` that encodes its state for
 *     the position `pos` it is currently used for:
 *
 *         seq == pos            slot is FREE   -> producer may write it
 *         seq == pos + 1        slot is FULL   -> a consumer may take it
 *         seq == pos + capacity slot was consumed; free for the next lap
 *
 *     That single atomic is the ONLY synchronisation on the payload:
 *       producer: write data,        seq.store(pos+1, RELEASE)
 *       consumer: seq.load(ACQUIRE), read data,  seq.store(pos+capacity, RELEASE)
 *     The release/acquire pairs give the happens-before edges, so the
 *     non-atomic Quote copy inside the slot is race-free. No mutex on hot path.
 *
 *  3. SINGLE producer  -> `writePos_` is a plain integer owned by the producer;
 *     no CAS and no sharing on the enqueue side.
 *     MULTIPLE consumers -> they race to claim a position with a CAS on
 *     `readPos_`. Only the CAS winner touches that slot's data, so consumers
 *     never read the same quote twice.
 *
 *  4. FALSE SHARING is avoided on purpose: writePos_ (producer), readPos_
 *     (consumers), epoch_ (park/unpark) and the read-mostly members each live
 *     on their own 64-byte line, and every slot is exactly one cache line so
 *     the producer writing slot N+1 never invalidates a consumer reading slot N.
 *
 *  5. BLOCKING dequeue ("dequeue should block if queue is empty"):
 *     spin briefly (cheap, lowest latency), then park the thread with C++20
 *     std::atomic::wait so idle consumers burn no CPU. The producer rings a
 *     doorbell (`epoch_`) after every publish. See dequeue() for why no wakeup
 *     can be lost.
 *
 *  6. SHUTDOWN: dequeue() would otherwise block forever, so the producer calls
 *     close() after its last enqueue. Consumers first DRAIN what is left, then
 *     dequeue() returns std::nullopt, letting the threads exit and be joined.
 *
 *  7. Quote is a fixed-size, trivially-copyable struct. A std::string timestamp
 *     is 27 chars (> SSO), i.e. a heap allocation per quote plus pointer
 *     chasing; a char[32] keeps the quote inline (48 bytes) and the slot
 *     copy a plain memcpy. ("Adjust DataStruct if required.")
 *
 *  Complexity: enqueue / dequeue are O(1), lock-free on the fast path.
 *  Bounded capacity => memory is fixed, no allocation after construction, and
 *  enqueue() applies back-pressure (spins/yields) if consumers fall behind.
 *  Use tryEnqueue() where the producer must never block (e.g. a feed handler).
 *
 *  Contract: exactly ONE thread may call enqueue()/tryEnqueue()/close(), and
 *  close() must come after the last enqueue. Any number of threads may call
 *  dequeue()/tryDequeue(). Destroy the queue only after all threads are joined.
 * ---------------------------------------------------------------------------
 */
#include <algorithm>    // std::max
#include <atomic>
#include <bit>          // std::bit_ceil
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64)
#include <immintrin.h>  // _mm_pause
#endif

// Typical x86/ARM cache line. (std::hardware_destructive_interference_size is
// avoided: GCC warns that it is ABI-unstable across compiler flags.)
inline constexpr std::size_t kCacheLine = 64;

// CPU hint for busy-wait loops: saves power, avoids memory-order machine
// clears on x86 and gives the sibling hyperthread the core.
inline void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

struct Quote {
    // Fixed-size, inline timestamp (see design note 7). A 27-char literal like
    // "2024-02-12 13:12:13:567.788" fits; a longer literal is a compile error.
    char   Timestamp[32] = {};
    double Price    = 0.;
    double Quantity = 0.;

    // Non-member friend, defined in-class: doesn't affect aggregate-ness, so
    // Quote{"ts", price, qty} keeps working.
    friend std::ostream& operator<<(std::ostream& os, const Quote& quote) {
        return os << "Timestamp: " << quote.Timestamp
                  << ", Price = "  << quote.Price
                  << ", Quantity = " << quote.Quantity;
    }
};
// Slots are copied with plain loads/stores; make sure that is actually legal.
static_assert(std::is_trivially_copyable_v<Quote>);

class QuoteQueue {
    // One slot == one cache line: 8 bytes seq + 48 bytes Quote + 8 bytes padding.
    struct alignas(kCacheLine) Slot {
        std::atomic<std::uint64_t> seq{0};
        Quote                      data;
    };

public:
    // capacity is rounded up to a power of two (>= 2) so that `pos & mask_`
    // replaces a modulo. It bounds how many quotes can be in flight.
    explicit QuoteQueue(const std::size_t capacity = 1024)
        : capacity_(std::bit_ceil(std::max<std::size_t>(capacity, 2))),
          mask_(capacity_ - 1),
          slots_(std::make_unique<Slot[]>(capacity_)) {
        // Slot i is initially "free for position i".
        for (std::size_t i = 0; i < capacity_; ++i) {
            slots_[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    QuoteQueue(const QuoteQueue&)            = delete;
    QuoteQueue& operator=(const QuoteQueue&) = delete;

    // ======================= PRODUCER SIDE (single thread) ==================

    // Non-blocking. Returns false (and leaves the queue untouched) if full.
    bool tryEnqueue(Quote quote) { return tryPublish(quote); }

    // Blocking: waits for a free slot if the ring is full (consumers are behind).
    // Spins briefly, then yields the CPU, so a stalled consumer group cannot
    // make the producer burn a core forever.
    void enqueue(Quote quote) {
        unsigned attempts = 0;
        while (!tryPublish(quote)) {
            if (++attempts < kProducerSpins) cpuRelax();
            else                             std::this_thread::yield();
        }
    }

    // Signals "no more quotes". Consumers drain the remaining items and then
    // dequeue() returns std::nullopt. Call once, after the last enqueue.
    void close() {
        closed_.store(true, std::memory_order_release);
        // Wake EVERY parked consumer so they can observe `closed_` and exit.
        epoch_.fetch_add(1, std::memory_order_seq_cst);
        epoch_.notify_all();
    }

    // ======================= CONSUMER SIDE (many threads) ===================

    // Non-blocking. std::nullopt if there is currently nothing to take.
    std::optional<Quote> tryDequeue() {
        std::uint64_t pos = readPos_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[pos & mask_];

            // ACQUIRE pairs with the producer's RELEASE store of seq: if we see
            // pos+1 we are guaranteed to also see the fully written Quote.
            const std::uint64_t seq = slot.seq.load(std::memory_order_acquire);
            const std::int64_t  dif = static_cast<std::int64_t>(seq) -
                                      static_cast<std::int64_t>(pos + 1);

            if (dif == 0) {
                // Slot holds the item for `pos`. Try to CLAIM that position.
                // relaxed is enough: the CAS only arbitrates between consumers;
                // data visibility already came from the acquire load above.
                if (readPos_.compare_exchange_weak(pos, pos + 1,
                                                   std::memory_order_relaxed)) {
                    // We own this slot exclusively until we release it below.
                    std::optional<Quote> result{slot.data};
                    // Hand the slot back to the producer for the next lap.
                    // RELEASE: our read of `data` must complete before the
                    // producer is allowed to overwrite it.
                    slot.seq.store(pos + capacity_, std::memory_order_release);
                    return result;
                }
                // CAS failed: another consumer claimed `pos`; compare_exchange
                // has reloaded `pos` with the fresh value -> just retry.
            } else if (dif < 0) {
                // seq == pos: the producer has not published position `pos` yet.
                // Single producer publishes in order, so the queue is EMPTY.
                return std::nullopt;
            } else {
                // seq > pos+1: someone else already consumed `pos` and the slot
                // was recycled. Our `pos` is stale; reload and retry.
                pos = readPos_.load(std::memory_order_relaxed);
            }
        }
    }

    // BLOCKS while the queue is empty. Returns std::nullopt only once the queue
    // has been close()d AND fully drained.
    std::optional<Quote> dequeue() {
        for (;;) {
            // Phase 1: spin. Quotes usually arrive within microseconds, and
            // parking/unparking costs a syscall + a scheduler wake-up.
            for (unsigned i = 0; i < kSpinBeforePark; ++i) {
                if (auto quote = tryDequeue()) return quote;
                cpuRelax();
            }

            // Phase 2: park until the producer rings the doorbell.
            //
            // LOST-WAKEUP SAFETY: we snapshot `epoch_` BEFORE the final emptiness
            // check. If the producer publishes after the snapshot it also bumps
            // epoch_, so wait(seen) sees epoch_ != seen and returns immediately
            // instead of sleeping. If it published before the snapshot, the
            // acquire load below makes that item visible to tryDequeue().
            const std::uint32_t seen = epoch_.load(std::memory_order_acquire);
            if (auto quote = tryDequeue()) return quote;

            if (closed_.load(std::memory_order_acquire)) {
                // Everything enqueued before close() is visible now. One last
                // look: nullopt here really means "closed and drained".
                return tryDequeue();
            }

            epoch_.wait(seen, std::memory_order_acquire);   // sleeps while epoch_ == seen
        }
    }

private:
    // Producer-only. Copies `quote` into the next slot and publishes it.
    bool tryPublish(Quote& quote) {
        Slot& slot = slots_[writePos_ & mask_];

        // Slot is free for this lap only when seq == writePos_. Otherwise it
        // still holds an unread quote from the previous lap => queue is FULL.
        // ACQUIRE pairs with the consumer's RELEASE in tryDequeue(), so the
        // consumer has finished reading before we overwrite the data.
        if (slot.seq.load(std::memory_order_acquire) != writePos_) return false;

        slot.data = std::move(quote);
        // Publish: RELEASE makes the data write visible to any consumer that
        // acquires seq == writePos_ + 1.
        slot.seq.store(writePos_ + 1, std::memory_order_release);
        ++writePos_;

        // Ring the doorbell for parked consumers. seq_cst (a full barrier, one
        // `lock xadd` on x86) is deliberate: the bump must be globally visible
        // BEFORE notify_one() checks for sleeping waiters. A plain release
        // store could sit in the store buffer while notify sees "no waiters"
        // and skips the wake -> a consumer would sleep with an item pending.
        // notify_one() is cheap (no syscall) when nobody is parked.
        epoch_.fetch_add(1, std::memory_order_seq_cst);
        epoch_.notify_one();
        return true;
    }

    // Tuning knobs.
    static constexpr unsigned kSpinBeforePark = 128;  // consumer spins before sleeping
    static constexpr unsigned kProducerSpins  = 64;   // producer spins before yielding

    // ---- read-mostly: shared by everyone, effectively constant --------------
    const std::size_t             capacity_;
    const std::size_t             mask_;
    const std::unique_ptr<Slot[]> slots_;
    std::atomic<bool>             closed_{false};    // written once, at shutdown

    // ---- each hot variable on its OWN cache line (no false sharing) --------
    alignas(kCacheLine) std::uint64_t              writePos_ = 0;  // producer only, non-atomic
    alignas(kCacheLine) std::atomic<std::uint64_t> readPos_{0};    // consumers CAS this
    // 32-bit on purpose: maps directly onto a futex word for atomic::wait.
    // It is only compared for (in)equality; wrap-around is harmless.
    alignas(kCacheLine) std::atomic<std::uint32_t> epoch_{0};      // park/unpark doorbell
};

int main() {
    constexpr int kQuotes    = 100;
    constexpr int kConsumers = 10;

    std::vector<std::thread> deqThreads;
    QuoteQueue queue;

    // The queue itself needs no lock. This mutex only stops the 10 consumers'
    // lines from interleaving on stdout in the demo.
    std::mutex coutMutex;
    std::atomic<int> consumed{0};

    std::thread enqThread([&queue] {
        int count = 0;
        while (count < kQuotes) {
            Quote quote({"2024-02-12 13:12:13:567.788", 100. + count, 50. + count * 0.5});
            queue.enqueue(std::move(quote));
            count++;
        }
        // Tell consumers no more quotes are coming; otherwise they'd block forever.
        queue.close();
    });

    for (int i = 0; i < kConsumers; i++) {
        deqThreads.emplace_back([&queue, &coutMutex, &consumed, i] {
            // dequeue() blocks if the queue is empty and yields nullopt once the
            // queue is closed and drained, which ends the loop.
            while (auto quote = queue.dequeue()) {
                {
                    std::lock_guard<std::mutex> lock(coutMutex);
                    std::cout << "[consumer " << i << "] " << *quote << '\n';
                }
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Wait for enqueue, dequeue threads to finish
    enqThread.join();
    for (auto& t : deqThreads) t.join();

    // Sanity check: every quote consumed exactly once.
    std::cout << "Consumed " << consumed.load() << " / " << kQuotes << " quotes\n";
    return consumed.load() == kQuotes ? 0 : 1;
}
