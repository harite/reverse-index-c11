/*
 * dichashset.h
 *  基础组件 hashset
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef DICHASHSET_H_
#define DICHASHSET_H_
#include <string.h>
#include "classdef.h"
#include "filestream.h"
#include "rwsyslock.h"
using namespace std;
namespace setint
{
	typedef long long int64;
	typedef unsigned int uint;
	template<class k> class block
	{
	private:
		k * keys;
		int size;
		int length;
		qstardb::rwsyslock rwlock;
		int indexof(k key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;

				if (keys[mid] < key)
					fromIndex = mid + 1;
				else if (keys[mid] > key)
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
				int newCapacity = minCapacity + 1 + (extCapacity > 1024 ? 1024 : extCapacity);
				k* temp = new k[newCapacity];
				memmove(temp, this->keys, sizeof(k) * this->size);
				delete[] this->keys;
				this->keys = temp;
				this->length = newCapacity;
			}
		}
	public:
		block()
		{
			this->size = 0;
			this->length = 16;
			this->keys = new k[this->length];
		}
		~block()
		{
			delete[] keys;
		}

		bool add(k key)
		{
			rwlock.wrlock();
			ensurecapacity(this->size + 1);
			if (this->size == 0 || keys[this->size - 1] < key)
			{
				keys[this->size++] = key;
				rwlock.unwrlock();
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
						memmove(this->keys + index + 1, this->keys + index, sizeof(k) * moveNum);
					}
					keys[index] = key;
					this->size++;
					rwlock.unwrlock();
					return true;
				}
				else
				{
					rwlock.unwrlock();
					return false;
				}
			}

		}
		bool remove(k key)
		{
			if (size == 0)
			{
				return false;
			}
			else
			{
				rwlock.wrlock();
				int index = indexof(key, qstardb::type_index);
				if (index >= 0)
				{
					int moveNum = size - index - 1;
					if (moveNum > 0)
					{
						memmove(this->keys + index, this->keys + index + 1, sizeof(k) * moveNum);
						this->size--;
					}
					else
					{
						this->keys[--size] = 0;
					}
					rwlock.unwrlock();
					return true;
				}
				else
				{
					rwlock.unwrlock();
					return false;
				}
			}

		}
		bool contains(k key)
		{
			rwlock.rdlock();
			bool result = indexof(key, qstardb::type_index) >= 0;
			rwlock.unrdlock();
			return result;
		}

		void readall(qstardb::filewriter& writer)
		{
			rwlock.rdlock();
			writer.writeInt32(this->size);
			for (int i = 0; i < this->size; i++)
			{
				writer.writeInt64(keys[i]);
			}
			rwlock.unrdlock();
		}
	};

	template<class k> class keyset
	{
	private:
		int mode;
		int size;
		block<k>* blocks;
		qstardb::rwsyslock rwlock;
		int index(k key)
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
	public:
		keyset(int mode)
		{
			this->size = 0;
			this->mode = mode > 0 ? mode : 16;
			this->blocks = new block<k> [this->mode];
		}
		void add(k key)
		{
			if (blocks[index(key)].add(key))
			{
				rwlock.wrlock();
				this->size++;
				rwlock.unwrlock();
			}
		}
		void remove(k key)
		{
			if (blocks[index(key)].remove(key))
			{
				rwlock.wrlock();
				this->size--;
				rwlock.unwrlock();
			}
		}

		bool contains(k key)
		{
			return blocks[index(key)].contains(key);
		}

		int keysize()
		{
			return this->size;
		}

		void writefile(string& filename)
		{
			if (this->size > 1024)
			{
				rwlock.wrlock();
				string temp = filename;
				temp.append(".temp");
				qstardb::filewriter fwriter(temp);
				fwriter.writeInt32(this->mode);
				for (int i = 0; i < this->mode; i++)
				{
					blocks[i].readall(fwriter);
				}
				fwriter.flush();
				fwriter.close();
				fwriter.reNameTo(filename);
				rwlock.unwrlock();
			}
		}
		void readfile(string& filename)
		{
			rwlock.wrlock();
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
						if (blocks[index(key)].add(key))
						{
							this->size++;
						}
					}
				}
			}
			rwlock.unwrlock();
		}
	};
}
#endif
