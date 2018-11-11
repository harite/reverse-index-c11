/*
 *  stopdecoder.h
 * 
 *  Created on: 2015年2月11日
 *  停止位解码
 *  Author: jkuang
 */
#ifndef  STOPDECODER_H_
#define  STOPDECODER_H_
#include <string.h>
namespace qstardb
{
	using namespace std;
	class stopdecoder
	{
		char* chs { null };
		int size { 0 };
		int offset { 0 };
		bool reverse { false };
		int64 curValue { 0 };
		int stackindex { 0 };
		int offsetstack[32];
		int64 valuestarck[32];
	public:
		stopdecoder() = default;
		stopdecoder(char* chs, int size, bool reverse)
		{
			this->reset(chs, size, reverse);
		}

		inline void reset(char* chs, int size, bool reverse)
		{
			this->size = size;
			this->chs = chs;
			this->reverse = reverse;
			this->curValue = 0;
			this->stackindex = 0;
			reverse ? this->offset = size : this->offset = 0;
		}

		inline bool hasNext()
		{
			return reverse ? (this->offset > 0) : (this->offset < size);
		}

		inline void mark()
		{
			this->offsetstack[stackindex] = this->offset;
			this->valuestarck[stackindex] = this->curValue;
			this->stackindex++;
		}

		inline void resetToHead()
		{
			this->offset = 0;
			this->curValue = 0;
		}

		inline void reset()
		{
			if (this->stackindex > 0)
			{
				this->stackindex--;
				this->offset = this->offsetstack[stackindex];
				this->curValue = this->valuestarck[stackindex];
			}
			else
			{
				this->offset = 0;
				this->curValue = 0;
			}
		}

		inline long long nextUint64()
		{
			return reverse ? reverseUint64Value() : nextUint64Value();
		}
		inline long long nextSum()
		{
			curValue += nextUint64();
			return curValue;
		}
		~stopdecoder() = default;

	private:
		inline long nextValue()
		{
			long long value = 0;
			int index = this->offset + 1;
			while (this->offset < size)
			{
				char ch = chs[offset++];
				if (ch < 0)
				{
					value |= (ch & 0x3f) << ((offset - index) * 7);
					if (ch >> 6 == -1)
					{
						return -value;
					}
					else
					{
						return value;
					}
				}
				else
				{
					value |= ch << ((offset - index) * 7);
				}
			}
			return value;
		}

		inline long long reverseValue()
		{

			if (this->offset <= 0)
			{
				return 0;
			}
			while (--this->offset >= 0 && chs[this->offset] >= 0)
			{
			}
			int tempoffset = this->offset;
			long long value = 0;
			int index = this->offset + 1;
			while (tempoffset < size)
			{
				char ch = chs[tempoffset++];
				if (ch < 0)
				{
					value |= (ch & 0x3f) << ((tempoffset - index) * 7);
					if (ch >> 6 == -1)
					{
						return -value;
					}
					else
					{
						return value;
					}
				}
				else
				{
					value |= ch << ((tempoffset - index) * 7);
				}
			}
			return value;
		}

		inline unsigned long long nextUint64Value()
		{
			unsigned long long value = 0;
			int index = this->offset + 1;
			while (this->offset < size)
			{
				char ch = chs[offset++];
				if (ch < 0)
				{
					value |= (ch & 0x7f) << ((offset - index) * 7);
					return value;
				}
				else
				{
					value |= ch << ((offset - index) * 7);
				}
			}
			return value;
		}

		inline unsigned long long reverseUint64Value()
		{

			if (this->offset <= 0)
			{
				return 0;
			}
			while (--this->offset >= 0 && chs[this->offset] >= 0)
			{
			}
			int tempoffset = this->offset;
			unsigned long long value = 0;
			int index = this->offset + 1;
			while (tempoffset < size)
			{
				char ch = chs[tempoffset++];
				if (ch < 0)
				{
					value |= (ch & 0x7f) << ((tempoffset - index) * 7);
					return value;
				}
				else
				{
					value |= ch << ((tempoffset - index) * 7);
				}
			}
			return value;
		}
	};
}
#endif
