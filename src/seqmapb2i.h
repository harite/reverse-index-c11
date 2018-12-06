#pragma once
#ifndef _SEQMAP_B2I_H
#define _SEQMAP_B2I_H
#include "sequence.h"
#include "seqmapi2b.h"
namespace seqmap 
{
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};
	inline uint strhash(const char* str, int size)
	{
		uint hash = 0;
		while (size-- > 0)
		{
			hash = hash * 31 + (*str++);
		}
		return hash;
	}

	inline int compare(const char* ch0, int len0, const char* ch1, int len1)
	{
		if (len0 == len1)
		{
			for (int i = 0; i < len1; i++)
			{
				if (ch0[i] > ch1[i])
				{
					return 1;
				}
				else if (ch0[i] < ch1[i])
				{
					return -1;
				}
			}
			return 0;
		}
		else
		{
			return len0 - len1;
		}
	}
	class b2ipage
	{
	public:
		int size{0};
		int length;
		uint* keys;
		seqblock* _block;
		int indexof(uint* keys, const char* key, int len, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int tempLength;
				const char* temp = _block->find(keys[mid],tempLength);
				int cmp = compare(temp, tempLength, key, len);
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
																	// found
			}
			switch (_type)
			{
			case type_insert:
				return fromIndex > size ? size : fromIndex;
			case type_ceil:
				return fromIndex;
			case type_index:
				return -(fromIndex + 1);
			default:
				return toIndex;
			}
		}
	public:
		b2ipage(int len)
		{
			this->length = len;
			this->keys = new uint[this->length];
		}
		~b2ipage() 
		{
			delete[] keys;
		}

		int inset(const char* ch,int len)
		{

		}

		int get(const char* ch,int len)
		{

		}

		bool remove(const char* ch,int len)
		{

		}
	};
}
#endif
