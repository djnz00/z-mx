//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series Statistics

// rolling mean, variance and standard deviation
// rolling median, percentiles (using order statistics tree)

#ifndef ZdfStats_HH
#define ZdfStats_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <math.h>

#include <stats_tree.hh>

#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuFP.hh>

#include <zlib/ZuFixed.hh>

#include <zlib/ZmHeap.hh>
#include <zlib/ZmAllocator.hh>

namespace Zdf {

namespace pbds = __gnu_pbds;

// rolling count, total, mean, variance, standard deviation
class Stats {
public:
  Stats() = default;
  Stats(const Stats &) = delete;
  Stats &operator =(const Stats &) = delete;
  Stats(Stats &&) = default;
  Stats &operator =(Stats &&) = default;
  ~Stats() = default;

  double fp(ZuFixedVal v) const {
    return ZuFixed{v, m_ndp}.fp();
  }

  unsigned count() const { return m_count; }
  double total() const { return fp(m_total); }
  double mean() const {
    if (ZuUnlikely(!m_count)) return 0.0;
    return fp(m_total) / double(m_count);
  }
  double var() const {
    if (ZuUnlikely(!m_count)) return 0.0;
    return (m_var / double(m_count)) / m_varMul;
  }
  // Note: std() returns the population standard deviation, i.e.
  // the equivalent of Excel's STDEVP(); by contrast, Excel's STDEV() uses
  // the n-1 formula intended for statistical sampling - this
  // implementation performs a running calculation on an entire series
  double std() const { return sqrt(var()); }
  unsigned ndp() const { return m_ndp; }

  void ndp(unsigned newExp) {
    auto exp = m_ndp;
    if (ZuUnlikely(newExp != exp)) {
      if (m_count) {
	if (newExp > exp) {
	  auto m = ZuDecimalFn::pow10_64(newExp - exp);
	  m_total *= m;
	  auto m_ = double(m);
	  m_var *= (m_ * m_);
	} else {
	  auto m = ZuDecimalFn::pow10_64(exp - newExp);
	  m_total /= m;
	  auto m_ = double(m);
	  m_var /= (m_ * m_);
	}
      }
      m_ndp = newExp;
      auto m = double(ZuDecimalFn::pow10_64(newExp));
      m_varMul = m * m;
    }
  }

  void add(const ZuFixed &v) {
    ndp(v.ndp());
    add_(v.mantissa());
  }
protected:
  void add_(ZuFixedVal v_) {
    if (ZuUnlikely(!m_count)) {
      m_total = v_;
    } else {
      double n = m_count;
      auto prev = double(m_total) / n;
      auto mean = double(m_total += v_) / (n + 1.0);
      auto v = double(v_);
      m_var += (v - prev) * (v - mean);
	// ZuFixed{fp(m_var) + (v - prev) * (v - mean), m_ndp}.mantissa();
    }
    ++m_count;
  }

public:
  void del(const ZuFixed &v) {
    del_(v.adjust(m_ndp));
  }
protected:
  void del_(ZuFixedVal v_) {
    std::cout << "del_(" << ZuBoxed(v_) << ")\n";
    if (ZuUnlikely(!m_count)) return;
    if (m_count == 1) {
      m_total = 0;
    } else if (m_count == 2) {
      m_total -= v_;
      m_var = 0.0;
    } else {
      double n = m_count;
      auto prev = double(m_total) / n;
      auto mean = double(m_total -= v_) / (n - 1.0);
      auto v = double(v_);
      m_var -= (v - prev) * (v - mean);
	// ZuFixed{fp(m_var) - (v - prev) * (v - mean), m_ndp}.mantissa();
    }
    --m_count;
  }

  void clean() {
    m_count = 0;
    m_total = 0;
    m_var = 0.0;
  }

private:
  unsigned	m_count = 0;
  ZuFixedVal	m_total = 0;
  double	m_var = 0.0;	// accumulated variance
  unsigned	m_ndp = 0;
  double	m_varMul = 1.0;
};

// NTP defaults
struct StatsTree_Defaults {
  static const char *HeapID() { return "Zdf.StatsTree"; }
};

// StatsTreeHeapID - the heap ID
template <auto HeapID_, class NTP = StatsTree_Defaults>
struct StatsTreeHeapID : public NTP {
  constexpr static auto HeapID = HeapID_;
};

template <class NTP = StatsTree_Defaults>
class StatsTree : public Stats {
public:
  constexpr static auto HeapID = NTP::HeapID;
  using Alloc = ZmAllocator<std::pair<ZuFixedVal, unsigned>, HeapID>;

private:
  using Tree = pbds::stats_tree<ZuFixedVal, unsigned, Alloc>;
  static Tree &tree_();
  static const Tree &ctree_();
public:
  using Iter = decltype(tree_().begin());
  using CIter = decltype(ctree_().begin());

public:
  StatsTree() = default;
  StatsTree(const StatsTree &) = delete;
  StatsTree &operator =(const StatsTree &) = delete;
  StatsTree(StatsTree &&) = default;
  StatsTree &operator =(StatsTree &&) = default;
  ~StatsTree() = default;

  unsigned ndp() { return Stats::ndp(); }
  void ndp(unsigned newExp) {
    auto exp = ndp();
    if (ZuUnlikely(newExp != exp)) {
      if (count()) {
	if (newExp > exp)
	  shiftLeft(ZuDecimalFn::pow10_64(newExp - exp));
	else
	  shiftRight(ZuDecimalFn::pow10_64(exp - newExp));
      }
      Stats::ndp(newExp);
    }
  }

  void shiftLeft(uint64_t f) {
    for (auto i = begin(); i != end(); ++i)
      const_cast<ZuFixedVal &>(i->first) *= f;
  }
  void shiftRight(uint64_t f) {
    for (auto i = begin(); i != end(); ++i)
      const_cast<ZuFixedVal &>(i->first) /= f;
  }

  void add(const ZuFixed &v_) {
    ndp(v_.ndp());
    auto v = v_.mantissa();
    add_(v);
  }
  void del(const ZuFixed &v_) {
    auto v = v_.adjust(ndp());
    auto iter = m_tree.find(v);
    if (iter != end()) del_(iter);
  }
  template <typename T>
  ZuIs<T, Iter> del(T iter) {
    if (iter != end()) del_(iter);
  }

  auto begin() const { return m_tree.begin(); }
  auto end() const { return m_tree.end(); }

  double fp(CIter iter) const {
    if (iter == end()) return ZuFP<double>::nan();
    return Stats::fp(iter->first);
  }

  double minimum() const { return fp(begin()); }
  double maximum() const {
    if (!count()) return ZuFP<double>::nan();
    return fp(--end());
  }

  template <typename T>
  auto find(T &&v) const { return m_tree.find(ZuFwd<T>(v)); }

  auto order(unsigned n) const {
    return m_tree.find_by_order(n);
  }
  // 0 <= n < 1
  auto rankIter(double n) const {
    return m_tree.find_by_order(n * double(this->count()));
  }
  // 0 <= n < 1
  double rank(double n) const {
    auto iter = rankIter(n);
    if (iter == m_tree.end()) return ZuFP<double>::nan();
    return fp(iter);
  }
  auto medianIter() const {
    return m_tree.find_by_order(this->count()>>1);
  }
  double median() const {
    auto iter = medianIter();
    if (iter == m_tree.end()) return ZuFP<double>::nan();
    return fp(iter);
  }

  void clean() {
    Stats::clean();
    m_tree.clear();
  }

private:
  void add_(ZuFixedVal v) {
    Stats::add_(v);
    m_tree.insert(v);
  }
  void del_(Iter iter) {
    Stats::del_(iter->first);
    m_tree.erase(iter);
  }

private:
  Tree		m_tree;
};

} // namespace Zdf

#endif /* ZdfStats_HH */
