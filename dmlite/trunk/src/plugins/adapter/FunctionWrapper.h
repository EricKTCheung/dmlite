/// @file   FunctionWrapper.h
/// @brief  Function wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef FUNCTIONWRAPPER_H
#define FUNCTIONWRAPPER_H

#include <errno.h>
#include <dmlite/cpp/exceptions.h>
#include <pthread.h>
#include <serrno.h>

namespace dmlite {

  /// Throw an exception from the serr value.
  void ThrowExceptionFromSerrno(int serr, const char* extra = 0x00) throw(DmException);
  
  /// Allocates and set the error buffers only once per thread.
  void wrapperSetBuffers(void);

  /// Function wrapper.
  /// Explanation on how this works:
  ///     When calling a function in C/C++, the first parameter in the function goes last into
  ///     the stack, so then the callee accesses those parameters as offsets from the bottom of the
  ///     stack. The caller frees the used space in the stack.
  ///     This means, if we pass four parameters to a function that only expects two, it doesn't really
  ///     matter much, since they are not going to be accessed by the callee, and the caller (who knows
  ///     how many were passed) will free the four anyway. So it may not be nice, but it is safe.
  ///     This is how variable args (...) works.
  ///     This template generalizes any function as if it had 10 parameters, being the type of each
  ///     a parameterized type, and considering the rest a char.
  ///     This idea is taken from Boost's approach to tuples.
  ///
  /// What for?
  ///     Have the wrapping logic in one single place for any kind of function:
  ///     Error buffer, exception throwing, etc...
  template <typename RType, typename P1 = char, typename P2 = char, typename P3 = char,
            typename P4 = char, typename P5 = char, typename P6 = char, typename P7 = char,
            typename P8 = char, typename P9 = char, typename P10 = char>
  class FunctionWrapper {
   private:
    P1 p1_;
    P2 p2_;
    P3 p3_;
    P4 p4_;
    P5 p5_;
    P6 p6_;
    P7 p7_;
    P8 p8_;
    P9 p9_;
    P10 p10_;
    RType (*fun_)(...);

    /// The trick: We use a template that expects a pointer, so when RType is one,
    /// this is called.
    template <class R>
    RType callImpl_(R*, int retries)
    {
      RType r;
      do {
        r = fun_(p1_, p2_, p3_, p4_, p5_, p6_, p7_, p8_, p9_, p10_);
        --retries;
      } while (r == NULL && retries > 0);
      if (r == NULL) ThrowExceptionFromSerrno(serrno);

      return r;
    }

    /// And when it is not a pointer, this is the called one.
    template <class R>
    RType callImpl_(R, int retries)
    {
      RType r;
      do {
        r = fun_(p1_, p2_, p3_, p4_, p5_, p6_, p7_, p8_, p9_, p10_);
        --retries;
      } while (r < 0 && retries > 0);
      if (r < 0) ThrowExceptionFromSerrno(serrno);
      return r;
    }

   public:
    typedef RType (*Function00)();
    typedef RType (*Function01)(P1);
    typedef RType (*Function02)(P1, P2);
    typedef RType (*Function03)(P1, P2, P3);
    typedef RType (*Function04)(P1, P2, P3, P4);
    typedef RType (*Function05)(P1, P2, P3, P4, P5);
    typedef RType (*Function06)(P1, P2, P3, P4, P5, P6);
    typedef RType (*Function07)(P1, P2, P3, P4, P5, P6, P7);
    typedef RType (*Function08)(P1, P2, P3, P4, P5, P6, P7, P8);
    typedef RType (*Function09)(P1, P2, P3, P4, P5, P6, P7, P8, P9);
    typedef RType (*Function10)(P1, P2, P3, P4, P5, P6, P7, P8, P9, P10);

    // Unluckily, we need a constructor per possible combination :(
    // With C++11 and variadic template parameters, we could do this whole business nicer
    FunctionWrapper(Function00 f) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function01 f, P1 p1): p1_(p1) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function02 f, P1 p1, P2 p2): p1_(p1), p2_(p2) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function03 f, P1 p1, P2 p2, P3 p3): p1_(p1), p2_(p2), p3_(p3) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function04 f, P1 p1, P2 p2, P3 p3, P4 p4): p1_(p1), p2_(p2), p3_(p3), p4_(p4) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function05 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function06 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function07 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function08 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function09 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8), p9_(p9) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapper(Function10 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9, P10 p10): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8), p9_(p9), p10_(p10) { fun_ = reinterpret_cast<RType(*)(...)>(f); };

    /// Sets the buffer once per thread, and call the method
    RType operator ()(int retries = 1)
    {
      wrapperSetBuffers();
      return callImpl_(RType(), retries);
    }
  };

  



  /// Function wrapper LE (less exceptions), which does a more responsible usage of exceptions.
  template <typename RType, typename P1 = char, typename P2 = char, typename P3 = char,
            typename P4 = char, typename P5 = char, typename P6 = char, typename P7 = char,
            typename P8 = char, typename P9 = char, typename P10 = char>
  class FunctionWrapperLE {
   private:
    P1 p1_;
    P2 p2_;
    P3 p3_;
    P4 p4_;
    P5 p5_;
    P6 p6_;
    P7 p7_;
    P8 p8_;
    P9 p9_;
    P10 p10_;
    RType (*fun_)(...);

    /// The trick: We use a template that expects a pointer, so when RType is one,
    /// this is called.
    template <class R>
    RType callImpl_(R*, int retries)
    {
      RType r;
      do {
        r = fun_(p1_, p2_, p3_, p4_, p5_, p6_, p7_, p8_, p9_, p10_);
        --retries;
      } while (r == NULL && retries > 0);
      if (r == NULL) ThrowExceptionFromSerrno(serrno);

      return r;
    }

    /// And when it is not a pointer, this is the called one.
    template <class R>
    RType callImpl_(R, int retries)
    {
      RType r;
      do {
        r = fun_(p1_, p2_, p3_, p4_, p5_, p6_, p7_, p8_, p9_, p10_);
        --retries;
      } while (r < 0 && retries > 0);
      //if (r < 0) ThrowExceptionFromSerrno(serrno);
      switch (r) {
        case ENOMEM:
        case EINVAL:
        case EFAULT:
        case E2BIG:
         ThrowExceptionFromSerrno(serrno);
      }
      
      return r;
    }

   public:
    typedef RType (*Function00)();
    typedef RType (*Function01)(P1);
    typedef RType (*Function02)(P1, P2);
    typedef RType (*Function03)(P1, P2, P3);
    typedef RType (*Function04)(P1, P2, P3, P4);
    typedef RType (*Function05)(P1, P2, P3, P4, P5);
    typedef RType (*Function06)(P1, P2, P3, P4, P5, P6);
    typedef RType (*Function07)(P1, P2, P3, P4, P5, P6, P7);
    typedef RType (*Function08)(P1, P2, P3, P4, P5, P6, P7, P8);
    typedef RType (*Function09)(P1, P2, P3, P4, P5, P6, P7, P8, P9);
    typedef RType (*Function10)(P1, P2, P3, P4, P5, P6, P7, P8, P9, P10);

    // Unluckily, we need a constructor per possible combination :(
    // With C++11 and variadic template parameters, we could do this whole business nicer
    FunctionWrapperLE(Function00 f) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function01 f, P1 p1): p1_(p1) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function02 f, P1 p1, P2 p2): p1_(p1), p2_(p2) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function03 f, P1 p1, P2 p2, P3 p3): p1_(p1), p2_(p2), p3_(p3) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function04 f, P1 p1, P2 p2, P3 p3, P4 p4): p1_(p1), p2_(p2), p3_(p3), p4_(p4) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function05 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function06 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function07 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function08 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function09 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8), p9_(p9) { fun_ = reinterpret_cast<RType(*)(...)>(f); };
    FunctionWrapperLE(Function10 f, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9, P10 p10): p1_(p1), p2_(p2), p3_(p3), p4_(p4), p5_(p5), p6_(p6), p7_(p7), p8_(p8), p9_(p9), p10_(p10) { fun_ = reinterpret_cast<RType(*)(...)>(f); };

    /// Sets the buffer once per thread, and call the method
    RType operator ()(int retries = 1)
    {
      wrapperSetBuffers();
      return callImpl_(RType(), retries);
    }
  };

}

#endif // FUNCTIONWRAPPER_H
