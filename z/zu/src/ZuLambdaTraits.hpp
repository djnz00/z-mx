//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// compile-time callable traits for lambdas
//
// - works with templated call operators (generic lambdas)

#ifndef ZuLambdaTraits_HPP
#define ZuLambdaTraits_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <zlib/ZuAssert.hpp>

#ifdef _MSC_VER
#pragma once
#endif
namespace ZuLambdaTraits {

// deduce args of a non-generic lambda - ArgList<L>
template <typename L, auto Fn> struct ArgList__; // undefined
template <
  typename L, typename R,
  typename ...FnArgs, R (*Fn)(FnArgs...)>
struct ArgList__<L, Fn> { using T = ZuTypeList<FnArgs...>; }; // static
template <
  typename L, typename R,
  typename ...FnArgs, R (L::*Fn)(FnArgs...)>
struct ArgList__<L, Fn> { using T = ZuTypeList<FnArgs...>; }; // mutable
template <
  typename L, typename R,
  typename ...FnArgs, R (L::*Fn)(FnArgs...) const>
struct ArgList__<L, Fn> { using T = ZuTypeList<FnArgs...>; }; // const
template <typename L, typename = void> struct ArgList_; // undefined
template <typename L> struct ArgList_<L, decltype(&L::operator(), void())> :
    public ArgList__<L, &L::operator()> { };
template <typename L> using ArgList = typename ArgList_<L>::T;

// deduce return type of lambda - Return<L, ArgList>
template <typename L, typename ArgList_> struct Return_;
template <typename L>
struct Return_<L, ZuTypeList<>> {
  using T = decltype(ZuDeclVal<L &>()());
};
template <typename L, typename ...Args>
struct Return_<L, ZuTypeList<Args...>> {
  using T = decltype(ZuDeclVal<L &>()(ZuDeclVal<Args>()...));
};
template <typename L, typename ArgList_ = ArgList<L>>
using Return = typename Return_<L, ArgList_>::T;

template <
  typename L, typename ArgList_ = ArgList<L>, typename = Return<L, ArgList_>>
struct IsVoidRet : public ZuFalse { };
template <typename L, typename ArgList_>
struct IsVoidRet<L, ArgList_, void> : public ZuTrue { };

// deduce mutability of lambda - IsMutable<L, ArgList>
template <typename L, typename = void>
struct IsMutable_NoArgs : public ZuTrue { };
template <typename L>
struct IsMutable_NoArgs<L, decltype(ZuDeclVal<const L &>()(), void())> :
    public ZuFalse { };
template <typename L, typename ArgList_, typename = void>
struct IsMutable_Args : public ZuTrue { };
template <typename L, typename ...Args>
struct IsMutable_Args<L, ZuTypeList<Args...>,
  decltype(ZuDeclVal<const L &>()(ZuDeclVal<Args>()...), void())> :
    public ZuFalse { };
template <typename L, typename ArgList_ = ArgList<L>>
struct IsMutable : public IsMutable_Args<L, ArgList_> { };
template <typename L>
struct IsMutable<L, ZuTypeList<>> : public IsMutable_NoArgs<L> { };

// deduce statefulness of lambda - IsStateless<L, ArgList>
template <typename ArgList_, typename FnArgList>
struct IsStateless_Fn__ : public ZuBool<ArgList_::N == FnArgList::N> { };
template <
  typename L, typename ArgList_,
  typename R, typename FnArgList, typename = void>
struct IsStateless_Fn_ : public ZuFalse { };
template <
  typename L, typename ArgList_,
  typename R, typename ...FnArgs>
struct IsStateless_Fn_<L, ArgList_, R, ZuTypeList<FnArgs...>,
  decltype(static_cast<R (*)(FnArgs...)>(ZuDeclVal<const L &>()), void())> :
    public IsStateless_Fn__<ArgList_, ZuTypeList<FnArgs...>> { };
template <typename L, typename ArgList_, auto Fn>
struct IsStateless_Fn : public ZuFalse { };
template <
  typename L, typename ArgList_,
  typename R, typename ...FnArgs, R (*Fn)(FnArgs...)> // static
struct IsStateless_Fn<L, ArgList_, Fn> :
    public IsStateless_Fn_<L, ArgList_, R, ZuTypeList<FnArgs...>> { };
template <
  typename L, typename ArgList_,
  typename R, typename ...FnArgs, R (L::*Fn)(FnArgs...)> // mutable
struct IsStateless_Fn<L, ArgList_, Fn> :
    public IsStateless_Fn_<L, ArgList_, R, ZuTypeList<FnArgs...>> { };
template <
  typename L, typename ArgList_,
  typename R, typename ...FnArgs, R (L::*Fn)(FnArgs...) const> // const
struct IsStateless_Fn<L, ArgList_, Fn> :
    public IsStateless_Fn_<L, ArgList_, R, ZuTypeList<FnArgs...>> { };
template <typename L, typename R, typename ArgList_, typename = void>
struct IsStateless_Generic_ : public ZuFalse { };
template <typename L, typename R, typename ...Args>
struct IsStateless_Generic_<L, R, ZuTypeList<Args...>,
  decltype(static_cast<R (*)(Args...)>(ZuDeclVal<const L &>()), void())> :
    public ZuTrue { };
template <typename L, typename = void>
struct IsStateless_Generic_NoArgs : public ZuFalse { };
template <typename L>
struct IsStateless_Generic_NoArgs<L, decltype(ZuDeclVal<L &>()(), void())> :
    public IsStateless_Generic_<L, Return<L, ZuTypeList<>>, ZuTypeList<>> { };
template <typename L, typename ArgList, typename = void>
struct IsStateless_Generic_Args : public ZuFalse { };
template <typename L, typename ...Args>
struct IsStateless_Generic_Args<L, ZuTypeList<Args...>,
  decltype(ZuDeclVal<L &>()(ZuDeclVal<Args>()...), void())> :
    public IsStateless_Generic_<
      L, Return<L, ZuTypeList<Args...>>, ZuTypeList<Args...>> { };
template <typename L, typename ArgList> struct IsStateless_Generic;
template <typename L>
struct IsStateless_Generic<L, ZuTypeList<>> :
    public IsStateless_Generic_NoArgs<L> { };
template <typename L, typename ...Args>
struct IsStateless_Generic<L, ZuTypeList<Args...>> :
    public IsStateless_Generic_Args<L, ZuTypeList<Args...>> { };
template <typename L, typename ArgList_ = ArgList<L>, typename = void>
struct IsStateless : public IsStateless_Generic<L, ArgList_> { };
template <typename L, typename ArgList_>
struct IsStateless<L, ArgList_, decltype(&L::operator(), void())> :
    public IsStateless_Fn<L, ArgList_, &L::operator()> { };

// convert stateless lambda to plain function

template <typename L, typename R, typename ArgList_> struct InvokeFnT_;
template <typename L, typename R, typename ...Args>
struct InvokeFnT_<L, R, ZuTypeList<Args...>> {
  typedef R (*T)(Args...);
};
template <
  typename L,
  typename ArgList_ = ArgList<L>,
  typename R = Return<L, ArgList_>>
using InvokeFnT = typename InvokeFnT_<L, R, ArgList_>::T;

} // ZuLambdaTraits

template <typename L> using ZuArgList = ZuLambdaTraits::ArgList<L>;

template <typename L, typename ArgList = ZuArgList<L>>
using ZuIsMutableLambda = ZuLambdaTraits::IsMutable<L, ArgList>;
template <typename L, typename ArgList = ZuArgList<L>>
using ZuIsVoidRetLambda = ZuLambdaTraits::IsVoidRet<L, ArgList>;
template <typename L, typename ArgList = ZuArgList<L>>
using ZuIsStatelessLambda = ZuLambdaTraits::IsStateless<L, ArgList>;
template <typename L, typename ArgList = ZuArgList<L>>
using ZuLambdaReturn = ZuLambdaTraits::Return<L, ArgList>;
template <typename L, typename ArgList = ZuArgList<L>>
using ZuInvokeFnT = ZuLambdaTraits::InvokeFnT<L, ArgList>;

template <typename L, typename ArgList = ZuArgList<L>>
constexpr auto ZuInvokeFn(const L &l) {
  return static_cast<ZuInvokeFnT<L>>(l);
}

// stateless lambdas are invoked using a placeholder this pointer...
// technically undefined behavior, but it elides preserving the this pointer
//
// this->x does not imply evaluating (*this).x (the reverse is true)
//
// C++23 static operator() would make all this redundant, but the ABI
// changes wreak havoc

template <typename L, typename ArgList = ZuArgList<L>, typename ...Args>
auto ZuInvokeLambda(Args &&... args) {
  struct Empty { };
  ZuAssert((ZuIsStatelessLambda<L, ArgList>{}));
  ZuAssert(sizeof(L) == sizeof(Empty));
  static Empty _;
  return reinterpret_cast<L *>(&_)->operator ()(ZuFwd<Args>(args)...);
}

template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuMutableLambda = ZuIfT<ZuIsMutableLambda<L, ArgList>{}, R>;
template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuNotMutableLambda = ZuIfT<!ZuIsMutableLambda<L, ArgList>{}, R>;
template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuVoidRetLambda = ZuIfT<ZuIsVoidRetLambda<L, ArgList>{}, R>;
template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuNotVoidRetLambda = ZuIfT<!ZuIsVoidRetLambda<L, ArgList>{}, R>;
template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuStatelessLambda = ZuIfT<ZuIsStatelessLambda<L, ArgList>{}, R>;
template <typename L, typename ArgList = ZuArgList<L>, typename R = void>
using ZuNotStatelessLambda = ZuIfT<!ZuIsStatelessLambda<L, ArgList>{}, R>;

#endif /* ZuLambdaTraits_HPP */
