#pragma once 

#include "common.hpp"
#include "block_info.hpp"
#include "bit_access.hpp"
#include "serializer.hpp"
#include <cstring>

namespace cindex
{
	template<typename BlockType = uint32_t>
	class bit_vector 
	{
	public:
		typedef BlockType block_type;

		bit_vector();
		bit_vector(const bit_vector<BlockType>& o);

		~bit_vector();

		void serialize(serializer& s) const;
		void deserialize(serializer& s);

		void clear();
		void clear_lazy();

		std::size_t
		size() const CINDEX_WARN_UNUSED_RESULT
		{
			return size_;
		}

		// single bit operations
		void
		push_back(bool b)
		{
			if (size_ == capacity_)
			{
				if (capacity_ == 0)
					capacity_ = 1;
				else
					capacity_ <<= 1;
				resize();
			}

			if (b)
				bit_access::set(buf_, size_);
			size_++;
		}

		void
		pop_back()
		{
			assert(size_ > 0);
			size_--;
			bit_access::unset(buf_, size_);
		}

		bool
		operator[](std::size_t i) const CINDEX_WARN_UNUSED_RESULT
		{
			assert(i < size_);
			return bit_access::get(buf_, i);
		}

		bool
		operator[](std::size_t i) CINDEX_WARN_UNUSED_RESULT
		{
			assert(i < size_);
			return bit_access::get(buf_, i);
		}

		// multiple bit operations
		template<typename SrcBlockType>
		void
		append(const bit_vector<SrcBlockType>& r, std::size_t i, std::size_t len)
		{
			assert(i + len <= r.size());
			std::size_t target_size = size_ + r.size();
			if (target_size > capacity_)
			{
				if (capacity_ == 0)
					capacity_ = 1;
				while (target_size > capacity_)
					capacity_ <<= 1;
				resize();
			}

			bit_access::copy_set(buf_, size_, r.buf_, i, len);
			size_ = target_size;
		}

		template<typename SrcBlockType>
		void
		append(const SrcBlockType* r, std::size_t i, std::size_t len)
		{
			std::size_t target_size = size_ + len;
			if (target_size > capacity_)
			{
				if (capacity_ == 0)
					capacity_ = 1;
				while (target_size > capacity_)
					capacity_ <<= 1;
				resize();
			}

			bit_access::copy_set(buf_, size_, r, i, len);
			size_ = target_size;
		}

		template<typename T>
		void
		append(T v, std::size_t len) //CINDEX_WARN_UNUSED_RESULT
		{
			std::size_t target_size = size_ + len;
			if (target_size > capacity_)
			{
				if (capacity_ == 0)
					capacity_ = 1;
				while (target_size > capacity_)
					capacity_ <<= 1;
				resize();
			}

			bit_access::copy_set(buf_, size_, &v, block_info<T>::bits_per_block - len, len);
			size_ = target_size;
		}

		template<typename T>
		T
		get(std::size_t i, std::size_t len) const //CINDEX_WARN_UNUSED_RESULT
		{
			assert(len <= block_info<T>::bits_per_block);

			T v = 0;
			bit_access::copy_set<T, block_type>(&v, block_info<T>::bits_per_block - len, buf_, i, len);
			return v;
		}

		// TODO: append consecutive 0's or 1's for fast exp-golomb coding encoding
		// TODO: count consecutive 0's or 1's starting at some position for faster exp-golomb coding decoding

		void compact();

		bit_vector<block_type>& operator=(const bit_vector<block_type>& o)
		{
			size_ = o.size_;
			capacity_ = o.capacity_;
			resize();
			memcpy(buf_, o.buf_, block_info<block_type>::size(size_));
			return *this;
		}

	protected:
		void resize();

	private:
		block_type* buf_;
		std::size_t size_;
		std::size_t capacity_;
	};
}

