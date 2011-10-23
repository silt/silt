#pragma once

#include "common.hpp"
#include "bit_access.hpp"

namespace cindex
{
	template<unsigned int Order = 0>
	class exp_golomb
	{
	public:
		template<typename T, typename BufferType>
		static void encode(BufferType& out_buf, const T& n)
		{
			BOOST_STATIC_ASSERT(sizeof(T) * 8 >= Order);

			T m;
			if (Order)
				m = (n >> Order) + 1;
			else
				m = n + 1;

			int len = 0;
			{
				T p = m;
				while (p)
				{
					len++;
					p >>= 1;
				}
			}

			for (int i = 1; i < len; i++)
				out_buf.push_back(0);

			assert(m >> (len - 1) == 1);
			out_buf.push_back(1);

			//for (int i = len - 2; i >= 0; i--)
			//	out_buf.push_back((m >> i) & 1);
			out_buf.append(m, static_cast<std::size_t>(len - 1));

			if (Order)
			{
				//for (int i = guarded_cast<int>(Order) - 1; i >= 0; i--)
				//	out_buf.push_back((n >> i) & 1);
				out_buf.append(n, static_cast<std::size_t>(Order));
			}
		}

		template<typename T, typename BufferType>
		static T decode(const BufferType& in_buf, std::size_t& in_out_buf_iter)
		{
			BOOST_STATIC_ASSERT(sizeof(T) * 8 >= Order);

			int len = 1;
			while (true)
			{
				assert(in_out_buf_iter < in_buf.size());
				if (in_buf[in_out_buf_iter++])
					break;
				len++;
			}

			T m = guarded_cast<T>(1) << (len - 1);
			//for (int i = len - 2; i >= 0; i--)
			//{
			//	assert(in_out_buf_iter < in_buf.size());
			//	if (in_buf[in_out_buf_iter++])
			//		m |= guarded_cast<T>(1) << i;
			//}
			assert(in_out_buf_iter + static_cast<std::size_t>(len - 1) <= in_buf.size());
			m |= in_buf.template get<T>(in_out_buf_iter, static_cast<std::size_t>(len - 1));	// "template" prefix is used to inform the compiler that in_buf.get is a member template
			in_out_buf_iter += static_cast<std::size_t>(len - 1);

			T n;
			if (Order)
			{
				n = (m - 1) << Order;
				assert(in_out_buf_iter + Order <= in_buf.size());
				//for (int i = guarded_cast<int>(Order) - 1; i >= 0; i--)
				//{
				//	if (in_buf[in_out_buf_iter++])
				//		n |= guarded_cast<T>(1) << i;
				//}
				n |= in_buf.template get<T>(in_out_buf_iter, static_cast<std::size_t>(Order));
				in_out_buf_iter += static_cast<std::size_t>(Order);
			}
			else
				n = m - 1;

			return n;
		}
	};
}

