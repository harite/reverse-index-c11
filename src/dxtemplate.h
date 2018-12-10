/*
 * dxtemplate.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef DXTEMPLATE_H_
#define DXTEMPLATE_H_
#include <iostream>
#include <algorithm>
#include "commons.h"
namespace qstardb
{

	using namespace std;
	typedef element2<uint, uint> twouint;
	typedef element3<uint, short, short> threeiss;
	typedef element3<uint, uint, uint> threeiii;
	typedef element3<uint, ushort, ushort> tnode;

	template<class c, class t, class s> inline int indexof(c* instance, t* keys, t key, s* _s, int size, type _type)
	{
		int fromIndex = 0;
		int toIndex = size - 1;
		while (fromIndex <= toIndex)
		{
			int mid = (fromIndex + toIndex) >> 1;
			int cmp = instance->compare(_s, keys[mid], key);
			if (cmp < 0)
				fromIndex = mid + 1;
			else if (cmp > 0)
				toIndex = mid - 1;
			else
				return _type == type_insert ? -(mid + 1) : mid; // key
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

	/**/
	template<class c, class t, class v> inline int indexof(c* instance, t* keys, v key, int size, type _type)
	{
		int fromIndex = 0;
		int toIndex = size - 1;
		while (fromIndex <= toIndex)
		{
			int mid = (fromIndex + toIndex) >> 1;
			int cmp = instance->compare(keys[mid], key);
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
	template<class c, class t, class v, class s> inline int indexof(c* instance, t* keys, v key, s* _s, int size, type _type)
	{
		int fromIndex = 0;
		int toIndex = size - 1;
		while (fromIndex <= toIndex)
		{
			int mid = (fromIndex + toIndex) >> 1;
			int cmp = instance->compare(_s, keys[mid], key);
			if (cmp < 0)
				fromIndex = mid + 1;
			else if (cmp > 0)
				toIndex = mid - 1;
			else
				return _type == type_insert ? -(mid + 1) : mid; // key
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

	template<class c> inline c* encapacity(c* old, int oldlen, int newlen)
	{
		c* temp = new c[newlen];
		if (oldlen > 0 && old !=  nullptr)
		{
			memcpy(temp, old, sizeof(c) * oldlen);
		}
		if (old !=  nullptr)
		{
			delete[] old;
		}
		return temp;
	}

	template<class c> inline c** encapacity(c** old, uint oldlen, uint newlen, uint rows)
	{
		c** temp = new c*[newlen];
		if (oldlen > 0 && old !=  nullptr)
		{
			memcpy(temp, old, sizeof(c*) * oldlen);
		}
		for (uint i = oldlen; i < newlen; i++)
		{
			temp[i] = new c[rows];
		}
		if (old !=  nullptr)
		{
			delete[] old;
		}
		return temp;
	}
	template<class c, class d> c* cleanup(int capacity, c* mems, d* nodes, uint size, threeiii& property)
	{
		c* temp = new c[capacity];
		if (property.v3 == 0)
		{
			memcpy(temp, mems, sizeof(c) * property.v1);
			delete[] mems;
			property.v2 = capacity;
			return temp;
		}
		else
		{
			cleanup(temp, mems, nodes, size, property);
			property.v2 = capacity;
			delete[] mems;
			return temp;
		}
	}

	template<class c, class d> c* cleanup(c* buffer, c* mems, d* nodes, uint size, threeiii& property)
	{

		if (property.v3 == 0)
		{
			memcpy(buffer, mems, sizeof(c) * property.v1);
			return buffer;
		}
		else
		{
			uint offset = 0;
			for (uint i = 0; i < size; i++)
			{
				int tsize = nodes[i].v2;
				if (tsize > 0)
				{
					int toffset = nodes[i].v1;
					if (tsize < 32)
					{
						for (int j = 0; j < tsize; j++)
						{
							buffer[offset + j] = mems[toffset + j];
						}
					}
					else
					{
						memcpy(buffer + offset, mems + toffset, sizeof(c) * tsize);
					}
					nodes[i].v1 = offset;
					offset += tsize;
				}
				else
				{
					nodes[i].v2 = 0;
					nodes[i].v1 = 0;
				}
			}
			property.v1 = offset;
			//property.second = capacity;
			property.v3 = 0;
			return buffer;
		}
	}

	template<class c, class d> c* optimize(c* mems, d* nodes, twouint& nodesps, threeiii& property)
	{
		if (nodesps.v1 == nodesps.v2)
		{
			if (property.v3 != 0 || property.v1 != property.v2)
			{
				int capacity = property.v1 - property.v3;
				mems = cleanup(capacity, mems, nodes, nodesps.v2, property);
			}
			return mems;
		}
		return mems;
	}

	template<class c, class d> c* datacp(uint _index, c* mems, const c* p, int len, d* nodes, twouint& nodesps, threeiii& property, bool isoptimize)
	{

		if (nodes[_index].v2 > 0)
		{
			cout << "  insert error! " << endl;
			return  nullptr;
		}
		if (-nodes[_index].v2 >= len)
		{
			memcpy(mems + nodes[_index].v1, p, sizeof(c) * len);
			nodes[_index].v2 = len;
			property.v3 -= len;
			nodesps.v1++;
			if (isoptimize)
			{
				return optimize(mems, nodes, nodesps, property);
			}
			else
			{
				return mems;
			}
		}
		else
		{
			bool hasmore = property.v2 > len + property.v1;
			int oldsize = property.v1;
			int odldll = property.v3;
			if (!hasmore && property.v3 > len && property.v3 * 100 > property.v2)
			{
				mems = cleanup(property.v2, mems, nodes, nodesps.v2, property);
			}
			else if (!hasmore)
			{
				int capacity = (property.v2 * 3 / 2) > len + property.v1 ? (property.v2 * 3 / 2) : len + property.v1;
				mems = cleanup(capacity, mems, nodes, nodesps.v2, property);
			}
			if (!hasmore && oldsize != property.v1 + odldll)
			{
				cout << "error in datacp!" << endl;
				return  nullptr;
			}
			memcpy(mems + property.v1, p, sizeof(c) * len);
			nodes[_index].v1 = property.v1;
			nodes[_index].v2 = len;
			property.v1 += len;
			nodesps.v1++;

			if (isoptimize)
			{
				return optimize(mems, nodes, nodesps, property);
			}
			else
			{
				return mems;
			}
		}
	}
}
#endif /* DXTEMPLATE_H_ */
