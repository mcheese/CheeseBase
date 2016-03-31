// Licensed under the Apache License 2.0 (see LICENSE file).

// Declares mutex and lock types.

#pragma once

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/shared_lock_guard.hpp>

namespace cheesebase {

using Mutex = boost::mutex;
using RwMutex = boost::shared_mutex;
using UgMutex = boost::upgrade_mutex;

template <class M>
using ExLock = boost::unique_lock<M>;

template <class M>
using ShLock = boost::shared_lock<M>;

template <class M>
using UgLock = boost::upgrade_lock<M>;
using Cond = boost::condition_variable;

template <class M>
using Guard = boost::lock_guard<M>;
template <class M>
using ShGuard = boost::shared_lock_guard<M>;
}

