// Licensed under the Apache License 2.0 (see LICENSE file).

#include <gsl.h>
#include <vector>

namespace cheesebase {

// Incremental Priority Queue.
// The priority in this queue is dynamic and depends on the last element
// extracted. Elements are returned in order of index, the next element to be
// extracted has the next highest or, if there is no higher one, the very
// lowest index. It is supposed to optimize the order of disk requests.
// The structure IS NOT thread-safe! Use external mutex if used parallel.
template<typename Index, class Value>
class IncPrioQueue {
public:
  IncPrioQueue(size_t reserve = 16)
  {
    m_data.reserve(reserve);
  }

  // Insert element into queue with regards to priority.
  void enqueue(Index index, Value element)
  {
    m_data.emplace_back(index, std::move(element));
    bubble_up(m_data.size() - 1);
  }

  // Get the first element and remove it from the queue.
  // Queue must NOT be empty before calling.
  Value dequeue()
  {
    Expects(!m_data.empty());

    auto ret = std::move(m_data.front());
    m_current = ret.first;

    if (m_data.size() > 1) {
      m_data[0] = std::move(m_data.back());
      m_data.pop_back();
      bubble_down(0);
    } else {
      m_data.clear();
    }

    return std::move(ret.second);
  }

  // Insert element and get the first from the queue. If the element to be
  // inserted has the highest priority no manipulation of the queue takes
  // place and it is returned right away.
  // Queue is allowed to be empty before calling.
  Value exchange(Index index, Value element)
  {
    if (m_data.empty() || compare(index, m_data.front().first)) {
      return element;
    } else {
      auto ret = std::move(m_data.front());
      m_current = ret.first;
      m_data[0] = { index, std::move(element) };
      bubble_down(0);
      return std::move(ret.second);
    }
  }

  // Return number of elements in the queue.
  size_t size() const
  {
    return m_data.size();
  }

private:
  // Returns true if first has a higher or equal priority than second.
  bool compare(Index first, Index second) const
  {
    if (first >= m_current) {
      return ((second >= m_current) ? first <= second : true);
    } else {
      return ((second >= m_current) ? false : first <= second);
    }
  }

  // Returns parent node position of given pos.
  static size_t parent(size_t pos)
  {
    Expects(pos > 0);
    return ((pos - 1) / 2);
  }
  
  static std::pair<size_t, size_t> childs(size_t pos)
  {
    auto c = pos * 2;
    return{ c + 1, c + 2 };
  }

  // Moves the element at pos up the heap to a fitting position.
  void bubble_up(size_t pos)
  {
    Expects(pos < m_data.size());
    while (pos > 0 && !compare(m_data[parent(pos)].first, m_data[pos].first)) {
      std::swap(m_data[parent(pos)], m_data[pos]);
      pos = parent(pos);
    }
  }
  
  // Moves the element at pos down the heap to a fitting position.
  void bubble_down(size_t pos)
  {
    Expects(pos < m_data.size());

    const auto size = m_data.size();
    for (;;) {
      auto c = childs(pos);
      size_t child;
      if (c.second >= size) {
        if (c.first >= size) return;
        child = c.first;
      } else {
        child = (compare(m_data[c.first].first, m_data[c.second].first)
                 ? c.first
                 : c.second);
      }

      if (compare(m_data[pos].first, m_data[child].first)) return;

      std::swap(m_data[pos], m_data[child]);
      pos = child;
    }
  }

  Index m_current{ 0 };
  std::vector<std::pair<Index,Value>> m_data;
};

}
