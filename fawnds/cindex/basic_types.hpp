#pragma once

#include "common.hpp"

#include <tr1/cstdint>
#include <boost/numeric/conversion/converter.hpp>
#include <climits>

namespace cindex
{
	// guarded static_cast
	template<typename T, typename S>
	inline T guarded_cast(const S& v)
	{
#ifndef NDEBUG
		return boost::numeric::converter<T, S>::convert(v);
#else
		return static_cast<T>(v);
#endif
	}

    typedef unsigned int uint128_t __attribute__((mode(TI)));
}

