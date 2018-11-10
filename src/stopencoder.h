/*
 *  stopencoder.h
 *
 *  Created on: 2015年2月11日
 *  基于停止位的编码
 *  Author: jkuang
 */
#ifndef  STOPENCODER_H_
#define  STOPENCODER_H_
#include <string.h>
/**
 *
 */
namespace qstardb
{

	class stopencoder
	{
		char* chs;
		int size;
		int offset { 0 };

	public:

		stopencoder(int size)
		{
			this->size = size;
			this->chs = new char[size];
		}

		char* getbuffer()
		{
			return chs;
		}

		void ensureCapacity(int minCapacity)
		{
			int oldCapacity = this->size;
			if (minCapacity > oldCapacity)
			{
				int newCapacity = (this->size >> 1) + oldCapacity + 1;
				if (newCapacity < minCapacity)
				{
					newCapacity = minCapacity;
				}
				char* temp = new char[newCapacity];
				memcpy(temp, this->chs, sizeof(char) * this->offset);
				delete[] chs;
				this->chs = temp;
			}
		}

		void reset()
		{
			this->offset = 0;
		}

		int getoffset()
		{
			return this->offset;
		}

		/*void write(long long vl)
		 {
		 ensureCapacity(this->offset +9);
		 char mark = 0x80;
		 if (vl < 0)
		 {
		 mark = 0xc0;
		 vl = -vl;
		 }
		 while (vl >> 7 > 0)
		 {
		 chs[this->offset++] = (char)(vl & 0x7f);
		 vl >>=  7;
		 }
		 if (vl >> 6 > 0)
		 {
		 chs[this->offset++] = (char)(vl & 0x7f);
		 chs[this->offset++] = mark;
		 }
		 else
		 {
		 chs[this->offset++] = (char)(mark | vl);
		 }
		 }*/

		void writeUInt64(unsigned long long vl)
		{
			ensureCapacity(this->offset + 10);
			while (vl >> 7 > 0)
			{
				chs[this->offset++] = (char) (vl & 0x7f);
				vl = vl >> 7;
			}
			chs[this->offset++] = (char) (0x80 | vl);
		}

		~stopencoder()
		{
			delete[] chs;
		}
	private:

	};
}
#endif
