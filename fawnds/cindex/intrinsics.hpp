#pragma once

#include "common.hpp"
#include "bit_access.hpp"

namespace cindex
{
	class intrinsics
	{
	public:
		template<typename T>
		static size_t clz(T n)
		{
			assert(false);
			return 0;

			//size_t len = 0;
			//while (n)
			//{
			//	len++;
			//	n >>= 1;
			//}
			//return sizeof(T) * CHAR_BIT - len;
		}
	};

	template<>
	inline size_t intrinsics::clz(unsigned char n)
	{
		return static_cast<size_t>(__builtin_clz(static_cast<unsigned int>(n))) - (sizeof(unsigned int) - sizeof(unsigned char)) * CHAR_BIT;
	}

	template<>
	inline size_t intrinsics::clz(char n)
	{
		return clz(static_cast<unsigned char>(n));
	}

	template<>
	inline size_t intrinsics::clz(unsigned short n)
	{
		return static_cast<size_t>(__builtin_clz(static_cast<unsigned int>(n))) - (sizeof(unsigned int) - sizeof(unsigned short)) * CHAR_BIT;
	}

	template<>
	inline size_t intrinsics::clz(short n)
	{
		return clz(static_cast<unsigned short>(n));
	}

	template<>
	inline size_t intrinsics::clz(unsigned int n)
	{
		return static_cast<size_t>(__builtin_clz(n));
	}

	template<>
	inline size_t intrinsics::clz(int n)
	{
		return clz(static_cast<unsigned int>(n));
	}

	template<>
	inline size_t intrinsics::clz(unsigned long n)
	{
		return static_cast<size_t>(__builtin_clzl(n));
	}

	template<>
	inline size_t intrinsics::clz(long n)
	{
		return clz(static_cast<unsigned long>(n));
	}

	template<>
	inline size_t intrinsics::clz(unsigned long long n)
	{
		return static_cast<size_t>(__builtin_clzll(n));
	}

	template<>
	inline size_t intrinsics::clz(long long n)
	{
		return clz(static_cast<unsigned long long>(n));
	}
}

