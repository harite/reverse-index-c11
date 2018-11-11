/*
 * dicmapi2i.h
 *  整形键值对，主要用于转化 64位整形到32位整形
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef DICMAPI2I_H_
#define DICMAPI2I_H_
#include <string.h>
#include <atomic>
#include <string>
#include "classdef.h"
#include "rwsyslock.h"
using namespace std;
namespace mapi2i
{
	typedef long long int64;

	template<class k, class v> class entry
	{
	public:
		k key;
		v value;
		entry() = default;
		~entry() = default;
		void set(k key, v value)
		{
			this->key = key;
			this->value = value;
		}
	};

	template<class k, class v> class i2iblock
	{
	private:
		int size { 0 };
		int length { 0 };
		entry<k, v>* entries { null };
		int indexof(k key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				if (entries[mid].key < key)
					fromIndex = mid + 1;
				else if (entries[mid].key > key)
					toIndex = mid - 1;
				else
					return _type == qstardb::type_insert ? -(mid + 1) : mid; // key
			}
			switch (_type)
			{
				case qstardb::type_insert:
					return fromIndex > this->size ? this->size : fromIndex;
				case qstardb::type_ceil:
					return fromIndex;
				case qstardb::type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}

		void ensurecapacity(int minCapacity)
		{
			int oldCapacity = this->length;
			if (minCapacity > oldCapacity)
			{
				int extCapacity = oldCapacity >> 1;
				int newCapacity = minCapacity + 1 + (extCapacity > 512 ? 512 : extCapacity);
				entry<k, v>* temp = new entry<k, v> [newCapacity];
				memmove(temp, this->entries, sizeof(entry<k, v> ) * this->size);
				delete[] this->entries;
				this->entries = temp;
				this->length = newCapacity;
			}
		}
	public:
		i2iblock()
		{
			this->length = 16;
			this->entries = new entry<k, v> [this->length];
		}
		~i2iblock()
		{
			delete[] entries;
		}

		bool add(k key, v value)
		{
			ensurecapacity(this->size + 1);
			if (this->size == 0 || entries[this->size - 1].key < key)
			{
				entries[this->size++].set(key, value);
				return true;
			}
			else
			{
				int index = indexof(key, qstardb::type_insert);
				if (index >= 0)
				{
					int moveNum = this->size - index;
					if (moveNum > 0)
					{
						memmove(this->entries + index + 1, this->entries + index, sizeof(entry<k, v> ) * moveNum);
					}
					entries[index].set(key, value);
					this->size++;
					return true;
				}
				else
				{
					return false;
				}
			}

		}
		bool remove(k key, v& value)
		{
			if (size == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(key, qstardb::type_index);
				if (index >= 0)
				{
					int moveNum = size - index - 1;
					value = this->entries[index].value;
					if (moveNum > 0)
					{
						memmove(this->entries + index, this->entries + index + 1, sizeof(entry<k, v> ) * moveNum);
					}
					size--;
					return true;
				}
				else
				{
					return false;
				}
			}

		}
		bool contains(k key)
		{
			return indexof(key, qstardb::type_index) >= 0;
		}

		bool get(k key, v& value)
		{
			int index = indexof(key, qstardb::type_index);
			if (index >= 0)
			{
				value = entries[index].value;
				return true;
			}
			return false;

		}
		void readall(qstardb::filewriter& writer)
		{
			writer.writeInt32(this->size);
			for (int i = 0; i < this->size; i++)
			{
				writer.writeInt64(entries[i].key);
				writer.writeInt64(entries[i].value);
			}
		}
	};

	template<class k, class v> class dici2i
	{
	private:
		int mode;
		atomic<int> size { 0 };
		i2iblock<k, v>* blocks;
		qstardb::rwsyslock rwlocks[64];
		inline int index(k key)
		{
			int value = key % mode;
			if (value < 0)
			{
				return -value;
			}
			else
			{
				return value;
			}
		}
		inline int _lock(k key)
		{
			return index(key) % 64;
		}
	public:
		dici2i(int mode)
		{
			this->mode = mode;
			this->blocks = new i2iblock<k, v> [mode];
		}
		~dici2i()
		{
			delete[] this->blocks;
		}
		void add(k key, v value)
		{
			rwlocks[_lock(key)].wrlock();
			if (blocks[index(key)].add(key, value))
			{
				this->size++;
			}
			rwlocks[_lock(key)].unwrlock();
		}
		bool remove(k key, v& value)
		{
			rwlocks[_lock(key)].wrlock();
			if (blocks[index(key)].remove(key, value))
			{
				this->size--;
				rwlocks[_lock(key)].unwrlock();
				return true;
			}
			else
			{
				rwlocks[_lock(key)].unwrlock();
				return false;
			}
		}

		bool get(k key, v& value)
		{
			rwlocks[_lock(key)].rdlock();
			bool result = blocks[index(key)].get(key, value);
			rwlocks[_lock(key)].unrdlock();
			return result;
		}

		bool contains(k key)
		{
			rwlocks[_lock(key)].rdlock();
			bool result = blocks[index(key)].contains(key);
			rwlocks[_lock(key)].unrdlock();
			return result;
		}

		int keysize()
		{
			return this->size;
		}

		void writefile(string& filename)
		{
			if (this->size > 1024)
			{
				string temp = filename;
				temp.append(".temp");
				qstardb::filewriter fwriter(temp);
				fwriter.writeInt32(this->mode);
				for (int i = 0; i < this->mode; i++)
				{
					rwlocks[i % 64].rdlock();
					blocks[i].readall(fwriter);
					rwlocks[i % 64].rdlock();
				}
				fwriter.flush();
				fwriter.close();
				fwriter.reNameTo(filename);
			}
		}

		void readfile(string& filename)
		{
			qstardb::filereader reader(filename);
			if (reader.isOpen())
			{
				int num = reader.readInt32();
				for (int i = 0; i < num; i++)
				{
					int segnum = reader.readInt32();
					for (int i = 0; i < segnum; i++)
					{
						int64 key = reader.readInt64();
						int64 value = reader.readInt64();
						if (blocks[index(key)].add(key, value))
						{
							this->size++;
						}
					}
				}
			}
		}
	};
}
#endif
