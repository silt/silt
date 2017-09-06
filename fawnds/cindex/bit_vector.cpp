#include "bit_vector.hpp"
#include <cstring>

namespace cindex
{
	template<typename BlockType>
	bit_vector<BlockType>::bit_vector()
		: buf_(NULL), size_(0), capacity_(8)
	{
		resize();
	}

	template<typename BlockType>
	bit_vector<BlockType>::bit_vector(const bit_vector<BlockType>& o)
		: buf_(NULL), size_(0), capacity_(8)
	{
		*this = o;
	}

	template<typename BlockType>
	bit_vector<BlockType>::~bit_vector()
	{
		clear();
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::serialize(serializer& s) const
	{
		s << size_;
		s << capacity_;

		s.write_or_fail(buf_, block_info<block_type>::size(size_));
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::deserialize(serializer& s)
	{
		s >> size_;
		s >> capacity_;

		free(buf_);
		buf_ = reinterpret_cast<block_type*>(malloc(block_info<block_type>::size(capacity_)));
		assert(buf_);
		s.read_or_fail(buf_, block_info<block_type>::size(size_));
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::clear()
	{
		free(buf_);
		buf_ = NULL;

		size_ = 0;
		capacity_ = 0;
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::clear_lazy()
	{
		size_ = 0;
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::compact()
	{
		capacity_ = size_;
		resize();
	}

	template<typename BlockType>
	void
	bit_vector<BlockType>::resize()
	{
		std::size_t old_byte_size = block_info<block_type>::size(size_);
		std::size_t new_byte_size = block_info<block_type>::size(capacity_);

		block_type* new_buf = reinterpret_cast<block_type*>(realloc(reinterpret_cast<void*>(buf_), new_byte_size));

		if (!new_buf)
		{
			assert(buf_);
			assert(new_byte_size > old_byte_size);

			new_buf = reinterpret_cast<block_type*>(malloc(new_byte_size));
			assert(new_buf);

			memcpy(new_buf, buf_, old_byte_size);

			free(buf_);
		}

		buf_ = new_buf;

		if (new_byte_size > old_byte_size)
			memset(reinterpret_cast<uint8_t*>(new_buf) + old_byte_size, 0, new_byte_size - old_byte_size);
	}

	template class bit_vector<uint8_t>;
	template class bit_vector<uint16_t>;
	template class bit_vector<uint32_t>;
	template class bit_vector<uint64_t>;
	template class bit_vector<char>;
	template class bit_vector<int8_t>;
	template class bit_vector<int16_t>;
	template class bit_vector<int32_t>;
	template class bit_vector<int64_t>;
}

