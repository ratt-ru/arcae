#ifndef ARCAE_FINALLY_H
#define ARCAE_FINALLY_H

#include <utility>

template <typename Fn>
struct Finally {
  Fn fn;
  bool enabled;
};

template <typename Fn>
Finally<Fn> finally(Fn&& fn) {
  return Finally<Fn>{std::forward<Fn>(fn), true};
}

#endif  // #define ARCAE_FINALLY_H
