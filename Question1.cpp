/*
 * Question 1
 * Optimize this code without the usage of external libraries.
 *  Parse this into a vector and print it to stdout
 * You may use the std libraries or STL, as well as any features through C++20
 */

/*
 * ---------------------------------------------------------------------------
 * WHAT WAS SLOW IN THE ORIGINAL, AND WHAT CHANGED
 * ---------------------------------------------------------------------------
 *  1. std::vector<std::string> copies every word into its own std::string.
 *     Anything longer than the SSO buffer (~15 chars) is a heap allocation.
 *     -> std::vector<std::string_view>: a word is just {pointer, length}
 *        into the original buffer. Zero copies, zero allocations per word.
 *
 *  2. The vector grew by repeated doubling (realloc + move of every element).
 *     -> Count the delimiters first (one cheap, vectorisable pass) and
 *        reserve() the exact size once.
 *
 *  3. find(" ") searches for a *string*; find(' ') searches for a *char*,
 *     which the library implements with memchr (SIMD in glibc).
 *
 *  4. words.at(i) does a bounds check on every access -> plain iteration.
 *
 *  5. std::endl == '\n' + flush(). That is one write() syscall PER WORD.
 *     -> Build the whole output in one pre-sized buffer, write it once.
 *
 *  6. originalText was a std::string constructed at runtime.
 *     -> constexpr std::string_view over the literal: nothing to construct.
 *
 * Behaviour is intentionally identical to the original: the delimiter is a
 * single ' ', so "a  b" yields {"a", "", "b"} and a trailing space yields a
 * trailing empty word. Output bytes are unchanged ("Hello\nWorld!\n").
 *
 * LIFETIME NOTE: the views point into `text`, so they are only valid while
 * that buffer is alive and unmodified. Here it is a string literal (static
 * storage), so that is trivially true.
 * ---------------------------------------------------------------------------
 */
#include <algorithm>    // std::count
#include <cstddef>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

// Splits `text` on `delim` into non-owning views. O(n) time, one allocation.
[[nodiscard]] std::vector<std::string_view>
split(std::string_view text, const char delim = ' ') {
    std::vector<std::string_view> words;

    // Exactly (#delimiters + 1) words are produced, so a single reserve()
    // guarantees no reallocation while pushing.
    words.reserve(static_cast<std::size_t>(
                      std::count(text.begin(), text.end(), delim)) + 1);

    std::size_t wordStart = 0;
    for (std::size_t pos = text.find(delim);          // char search -> memchr
         pos != std::string_view::npos;
         pos = text.find(delim, wordStart)) {
        words.emplace_back(text.substr(wordStart, pos - wordStart));  // no copy
        wordStart = pos + 1;
    }
    // Last word (also handles text with no delimiter, and the empty string).
    words.emplace_back(text.substr(wordStart));
    return words;
}

// Writes every word followed by '\n' using ONE pre-sized buffer and ONE write,
// instead of one flushing operator<< (std::endl) per word.
void printWords(const std::vector<std::string_view>& words) {
    std::size_t totalBytes = 0;
    for (const std::string_view w : words) totalBytes += w.size() + 1;  // +1 for '\n'

    std::string out;
    out.reserve(totalBytes);              // single allocation, no regrowth
    for (const std::string_view w : words) {
        out.append(w);
        out.push_back('\n');              // '\n', NOT std::endl (no flush)
    }
    std::cout.write(out.data(), static_cast<std::streamsize>(out.size()));
}

int main() {
    // constexpr string_view over a literal: no std::string construction at all.
    constexpr std::string_view originalText = "Hello World!";

    const std::vector<std::string_view> words = split(originalText);
    printWords(words);

    return 0;
}
