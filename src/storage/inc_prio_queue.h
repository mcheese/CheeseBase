// Licensed under the Apache License 2.0 (see LICENSE file).

#include <mutex>
#include <vector>

namespace cheesebase {

// Incremental Priority Queue.
// The priority in this queue is dynamic and depends on the last element
// extracted. Elements are returned in order of index, the next element to be
// extracted has the next highest or, if there is no higher one, the very
// lowest index. It is supposed to optimize the order of disk requests.
template<typename Index, class Value>
class IncPrioQueue {
public:
  IncPrioQueue() = default;

  // Insert element into queue with regards to priority.
  void enqueue(Index index, Value element)
  {

  }

  // Get the first element and remove it from the queue.
  Value dequeue()
  {
    return Value{};
  }

  // Insert element and get the first from the queue. If the element to be
  // insertet has the highest priority no manipulation of the queue takes
  // place and it is returned right away.
  Value exchange(Index index, Value element)
  {
    return Value{};
  }

  // Return number of elements in the queue.
  size_t size() const
  {
    return 0;
  }

private:
  // Returns true if first has a higher or equal priority than second.
  bool compare(Index first, Index second) const
  {
    return true;
  }

  Index m_current;
  std::mutex m_mtx;
  std::vector<std::pair<Index,Value>> m_data;
};

}
