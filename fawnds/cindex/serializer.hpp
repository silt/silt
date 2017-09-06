#pragma once

#include "common.hpp"
#include <vector>
#include <cstdio>

namespace cindex
{
	class serializer
	{
	public:
		typedef long long int offset_t;

		serializer(FILE* fp, offset_t& offset)
			: fp_(fp), offset_(offset), failed_(false)
		{
		}

		serializer(const serializer& r)
			: fp_(r.fp_), offset_(r.offset_), failed_(r.failed_)
		{
		}

		serializer& operator=(const serializer& r)
		{
			fp_ = r.fp_;
			offset_ = r.offset_;
			failed_ = r.failed_;
			return *this;
		}

		bool failed() const { return failed_; }

		template<typename T>
		serializer& operator<<(const T& v)
		{
			v.serialize(*this);
			return *this;
		}

		template<typename T>
		serializer& operator>>(T& v)
		{
			v.deserialize(*this);
			return *this;
		}

		void write_or_fail(const void* buf, std::size_t count)
		{
			if (::fwrite(buf, count, 1, fp_) != 1)
				failed_ = true;
			offset_ += static_cast<offset_t>(count);
		}

		void read_or_fail(void* buf, std::size_t count)
		{
			if (::fread(buf, count, 1, fp_) != 1)
				failed_ = true;
			offset_ += static_cast<offset_t>(count);
		}

	protected:
		FILE* fp_;
		offset_t& offset_;
		bool failed_;
	};

#define DEF_BASIC_TYPE(T)											\
	template<>														\
	inline serializer& serializer::operator<< <T>(const T& v)		\
	{																\
		write_or_fail(&v, sizeof(T));								\
		return *this;												\
	}																\
	template<>														\
	inline serializer& serializer::operator>> <T>(T& v)				\
	{																\
		read_or_fail(&v, sizeof(T));								\
		return *this;												\
	}

	DEF_BASIC_TYPE(uint8_t)
	DEF_BASIC_TYPE(uint16_t)
	DEF_BASIC_TYPE(uint32_t)
	DEF_BASIC_TYPE(uint64_t)
	DEF_BASIC_TYPE(char)
	DEF_BASIC_TYPE(int8_t)
	DEF_BASIC_TYPE(int16_t)
	DEF_BASIC_TYPE(int32_t)
	DEF_BASIC_TYPE(int64_t)
	DEF_BASIC_TYPE(float)
	DEF_BASIC_TYPE(double)
#undef DEF_BASIC_TYPE
}

