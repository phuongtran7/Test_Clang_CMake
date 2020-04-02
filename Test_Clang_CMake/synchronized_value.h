#pragma once
#include <mutex>

template <class T, class Mutex = std::mutex>
class synchronized_value
{
public:
    synchronized_value() = default;

    template <class Fn, class V>
    friend auto apply(Fn&& fn, synchronized_value<V>& svs) -> decltype(fn(std::declval<V&>()));

private:
    T value_;
    Mutex mtx_;
};

template <class Fn, class V>
auto apply(Fn&& fn, synchronized_value<V>& svs) -> decltype(fn(std::declval<V&>())) {
    std::lock_guard<std::mutex> guard{ svs.mtx_ };
    return fn(svs.value_);
}

template class synchronized_value<std::string>;