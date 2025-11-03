// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>

#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"

///-------------------------

namespace mytest {
// Thread-safe bounded queue
template <typename T>
class BoundedQueue {
  std::queue<T> q;
  std::mutex m;
  std::condition_variable cv_full, cv_empty;
  size_t capacity;
  bool done = false;

 public:
  explicit BoundedQueue(size_t cap) : capacity(cap) {}

  void push(T value) {
    std::unique_lock lock(m);
    cv_full.wait(lock, [&] { return q.size() < capacity; });
    q.push(std::move(value));
    cv_empty.notify_one();
  }

  std::optional<T> pop() {
    std::unique_lock lock(m);
    cv_empty.wait(lock, [&] { return done || !q.empty(); });
    if (q.empty()) return std::nullopt;
    T value = std::move(q.front());
    q.pop();
    cv_full.notify_one();
    return value;
  }

  void close() {
    std::lock_guard lock(m);
    done = true;
    cv_empty.notify_all();
  }
};

struct WorkItem {
  size_t index;
  std::string data;
};

struct Result {
  size_t index;
  std::optional<ad_utility::GeometryInfo> output;
};

// Simulated expensive computation
inline Result process(const WorkItem& item) {
  return {item.index, ad_utility::GeometryInfo::fromWktLiteral(item.data)};
}

}  // namespace mytest

//----------------------------------------------------

// A `GeoVocabulary` holds Well-Known Text (WKT) literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes for example the computation of bounding boxes for accelerated
// spatial queries. See the `GeometryInfo` class for details. Note: A
// `GeoVocabulary` is only suitable for WKT literals, therefore it should be
// used as part of a `SplitVocabulary`.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  using GeometryInfo = ad_utility::GeometryInfo;

  UnderlyingVocabulary literals_;

  // The file in which the additional information on the geometries (like
  // bounding box) is stored.
  ad_utility::File geoInfoFile_;

  // TODO<ullingerc> Possibly add in-memory cache of bounding boxes here

  // Filename suffix for geometry information file
  static constexpr std::string_view geoInfoSuffix = ".geoinfo";

  // Offset per index inside the geometry information file
  static constexpr size_t geoInfoOffset = sizeof(GeometryInfo);

  // Serialized version of `GeometryInfo`
  using GeometryInfoBuffer = std::array<uint8_t, geoInfoOffset>;

  // For an invalid WKT literal, the serialized geometry info is all-zero
  static constexpr GeometryInfoBuffer invalidGeoInfoBuffer = {};

  // Offset for the header of the geometry information file
  static constexpr size_t geoInfoHeader =
      sizeof(ad_utility::GEOMETRY_INFO_VERSION);

 public:
  GeoVocabulary() = default;

  // Load the precomputed `GeometryInfo` object for the literal with
  // the given index from disk. Return `std::nullopt` for invalid geometries.
  std::optional<GeometryInfo> getGeoInfo(uint64_t index) const;

  // Construct a filename for the geo info file by appending a suffix to the
  // given filename.
  static std::string getGeoInfoFilename(std::string_view filename) {
    return absl::StrCat(filename, geoInfoSuffix);
  }

  // Forward all the standard operations to the underlying literal vocabulary.
  // See there for more details.

  // ___________________________________________________________________________
  decltype(auto) operator[](uint64_t id) const { return literals_[id]; }

  // ___________________________________________________________________________
  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.lower_bound(word, comparator);
  }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.upper_bound(word, comparator);
  }

  // ___________________________________________________________________________
  UnderlyingVocabulary& getUnderlyingVocabulary() { return literals_; }

  // ___________________________________________________________________________
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return literals_;
  }

  // ___________________________________________________________________________
  void open(const std::string& filename);

  // Custom word writer, which precomputes and writes geometry info along with
  // the words.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File geoInfoFile_;
    size_t numInvalidGeometries_ = 0;
    size_t numInvalidPolygonArea_ = 0;

    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t queue_capacity = 1000;

    mytest::BoundedQueue<mytest::WorkItem> work_queue =
        mytest::BoundedQueue<mytest::WorkItem>(queue_capacity);
    std::mutex result_mutex;
    std::condition_variable result_cv;
    std::map<size_t, mytest::Result> results;
    std::atomic<size_t> next_to_write = 0;
    bool done_producing = false;
    bool done_processing = false;
    std::vector<std::thread> workers;
    std::thread writer;

   public:
    // Initialize the `geoInfoFile_` by writing its header and open a word
    // writer on the underlying vocabulary.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename);

    // Add the next literal to the vocabulary, precompute additional information
    // using `GeometryInfo` and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writer and close the `geoInfoFile_`
    // file handle. After this no more calls to `operator()` are allowed.
    void finishImpl() override;

    ~WordWriter() override;
  };

  // ___________________________________________________________________________
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename);
  }

  // ___________________________________________________________________________
  void close();
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
