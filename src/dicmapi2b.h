/*
 * dicmapi2b.h
 *  用于存储数据缓存的hashmap，目标为内容较大的二进制数据，如附件，文章
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef DICMAPI2B_H_
#define DICMAPI2B_H_
#include <string.h>
#include <set>
#include <map>
#include <atomic>
#include "rwsyslock.h"
#include "filestream.h"
#ifndef null
#define  null nullptr
#endif
using namespace std;
namespace mapi2b
{
	typedef long long int64;
	typedef unsigned int uint;
	static const int inithits = 8;
	uint _inthash(int64 key)
	{
		return key & 0xffffffff;
	}

	uint _inthash(int key)
	{
		return key & 0xffffffff;
	}
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};
	class entry
	{
	public:
		int64 key { 0 };
		uint hits { inithits }; //命中次数
		int length { 0 };
		char* ch { null };
		entry() = default;
		~entry() = default;
		void setKey(int64 key)
		{
			this->key = key;
			this->hits = 8;
		}
		void setValue(signed char* str, int len, bool createNew)
		{
			if (ch == null || createNew)
			{
				this->length = len;
				this->ch = new char[this->length];
				memmove(ch, str, this->length);
			} //内存相差不大的情况下
			else if (len <= this->length && len * 2 > this->length)
			{
				this->length = len;
				memmove(ch, str, this->length);
			}
			else //新空间占用不到原空间一半  或 新空间大于原有存储空间
			{
				this->free();
				this->length = len;
				this->ch = new char[this->length];
				memmove(ch, str, this->length);
			}
		}
		void reset()
		{
			//每次讲数量衰减一半
			this->hits = hits >> 1;
		}

		char* getValue()
		{
			this->hits++;
			return ch;
		}

		void free()
		{
			if (ch != null)
			{
				delete[] ch;
				ch = null;
			}
		}
	};

	class page
	{
	private:
		int size;
		int length;
		uint hits;
		entry* nodes;
		qstardb::rwsyslock rwlock;
		int indexof(int64 key, type _type)
		{
			int fromIndex = 0;
			int toIndex = this->size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;

				if (nodes[mid].key < key)
					fromIndex = mid + 1;
				else if (nodes[mid].key > key)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
			}
			switch (_type)
			{
				case type_insert:
					return fromIndex > this->size ? this->size : fromIndex;
				case type_ceil:
					return fromIndex;
				case type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}
	public:
		page()
		{
			this->hits = 0;
			this->size = 0;
			this->length = 8;
			this->nodes = new entry[64];
		}
		~page()
		{
			for (int i = 0; i < size; i++)
			{
				this->nodes[i].free();
			}
			delete[] this->nodes;
		}
		bool insert(int64 key, signed char* ch, int len)
		{
			rwlock.wrlock();
			if (this->size == this->length)
			{
				this->length += 64;
				entry* temp = new entry[this->length];
				memmove(temp, nodes, sizeof(entry) * this->size);
				delete[] this->nodes;
				this->nodes = temp;
			}

			if (this->size == 0 || nodes[this->size - 1].key < key)
			{
				this->nodes[this->size].setKey(key);
				this->nodes[this->size].setValue(ch, len, true);
				this->size++;
				rwlock.unwrlock();
				return true;
			}
			else
			{
				int index = indexof(key, type_insert);
				if (index < 0)
				{
					index = -index - 1;
					nodes[index].setValue(ch, len, false);
					rwlock.unwrlock();
					return false;
				}
				else
				{
					int moveNum = this->size - index;
					if (moveNum > 0)
					{
						memmove(nodes + index + 1, nodes + index, sizeof(entry) * moveNum);
					}
					this->nodes[index].setKey(key);
					this->nodes[index].setValue(ch, len, true);
					this->size++;
					rwlock.unwrlock();
					return true;
				}
			}

		}

		bool remove(int64 key)
		{
			if (size == 0)
			{
				return false;
			}
			rwlock.wrlock();
			int index = indexof(key, type_index);
			if (index < 0)
			{
				rwlock.unwrlock();
				return false;
			}
			else
			{
				int moveNum = this->size - index - 1;
				this->nodes[index].free();
				if (moveNum > 0)
				{
					memmove(this->nodes + index, this->nodes + index + 1, sizeof(entry) * moveNum);
				}
				this->size--;
				if (this->length > (this->size + 3) * 3)
				{
					this->length = (this->size + 3) * 2;
					entry* temp = new entry[this->length];
					memmove(temp, nodes, sizeof(entry) * this->size);
					delete[] nodes;
					this->nodes = temp;
				}
				rwlock.unwrlock();
				return true;
			}
		}

		bool get(int64 key, qstardb::charwriter& writer)
		{
			if (size == 0)
			{
				return false;
			}
			rwlock.rdlock();
			int index = indexof(key, type_index);
			if (index >= 0)
			{
				this->hits++;
				const char* temp = nodes[index].getValue();
				writer.writeInt64(key);
				writer.writeInt(nodes[index].length);
				writer.write(temp, 0, nodes[index].length);
				rwlock.unrdlock();
				return true;
			}
			else
			{
				rwlock.unrdlock();
				return false;
			}
		}
		char* get(int64 key, int& length)
		{
			if (size == 0)
			{
				length = 0;
				return null;
			}
			rwlock.rdlock();
			int index = indexof(key, type_index);
			if (index >= 0)
			{
				this->hits++;
				length = nodes[index].length;
				const char* temp = nodes[index].getValue();
				char* result = new char[length];
				memcpy(result, temp, sizeof(char) * length);
				rwlock.unrdlock();
				return result;
			}
			else
			{
				rwlock.unrdlock();
				length = 0;
				return null;
			}
		}

		int delhalf()
		{
			if (this->size > 16)
			{
				rwlock.wrlock();
				int halfsize = this->size >> 1;
				set<int64> delsize;
				bool* delmark = new bool[this->size];
				for (int i = 0; i < this->size; i++)
				{
					int64 index = ((int64) this->nodes[i].hits << 32) | i;
					delmark[i] = false;
					delsize.insert(index);
				}
				int delcount = 0;
				for (int64 key : delsize)
				{
					int index = key & 0xffffffff;
					delmark[index] = true;
					if (delcount++ > halfsize)
					{
						break;
					}
				}
				entry* temp = new entry[this->size];
				int offset = 0;
				for (int i = 0; i < this->size; i++)
				{
					if (!delmark[i])
					{
						memmove(temp + offset++, nodes + i, sizeof(entry));
					}
				}
				int oldsize = this->size;
				entry* old = this->nodes;
				this->nodes = temp;
				this->length = oldsize;
				this->size = offset;
				rwlock.unwrlock();
				for (int i = 0; i < oldsize; i++)
				{
					if (delmark[i])
					{
						old[i].free();
					}
				}
				delete[] old;
				delete[] delmark;
				return oldsize - offset;
			}
			else
			{
				return 0;
			}
		}
		void readall(qstardb::filewriter& writer)
		{
			rwlock.rdlock();
			writer.writeInt32(this->size);
			for (int i = 0; i < this->size; i++)
			{
				writer.writeInt64(nodes[i].key);
				writer.writeInt32(nodes[i].length);
				writer.writeBytes(nodes[i].ch, nodes[i].length);
			}
			rwlock.unrdlock();
		}
	};

	class dici2b
	{
	private:
		//int size;

		uint hashmode;
		page* pages;
		std::atomic<int> size { 0 };
		qstardb::rwsyslock rwlocks[64];
		uint index(int64 key)
		{
			return _inthash(key) % hashmode;
		}
		inline int _lock(int64 key)
		{
			return index(key) % 64;
		}
		inline void sort(int64* arr, int length)
		{
			for (int i = 0; i < length - 1; i++)
			{
				bool flag = true;
				for (int j = 0; j < length - 1 - i; j++)
				{
					if (index(arr[j]) > index(arr[j + 1]))
					{
						int v = arr[j];
						arr[j] = arr[j + 1];
						arr[j + 1] = v;
						flag = false;
					}
				}
				if (flag)
				{
					break;
				}
			}
		}
	public:
		dici2b(int hashmode)
		{
			this->size = 0;
			this->hashmode = hashmode;
			this->pages = new page[hashmode];
		}
		~dici2b()
		{
			delete[] this->pages;
		}

		int remove(int64 key)
		{
			rwlocks[_lock(key)].wrlock();
			if (pages[index(key)].remove(key))
			{
				this->size--;
			}
			rwlocks[_lock(key)].unwrlock();
			return this->size;
		}

		int insert(int64 key, signed char* ch, int len)
		{
			rwlocks[_lock(key)].wrlock();
			if (pages[index(key)].insert(key, ch, len))
			{
				this->size++;
			}
			rwlocks[_lock(key)].unwrlock();
			return this->size;
		}

		char* get(int64 key, int& len)
		{
			rwlocks[_lock(key)].rdlock();
			char* ch = pages[index(key)].get(key, len);
			rwlocks[_lock(key)].unrdlock();
			return ch;
		}

		int get(int64* keys, int length, qstardb::charwriter& writer)
		{
			int hits = 0;
			sort(keys, length);
			for (int i = 0; i < length; i++)
			{
				rwlocks[i % 64].rdlock();
				if (pages[index(keys[i])].get(keys[i], writer))
				{
					hits++;
				}
				rwlocks[i % 64].unrdlock();
			}
			return hits;
		}

		int delhalf()
		{
			int sum = 0;
			for (int i = this->hashmode - 1; i >= 0; i--)
			{
				rwlocks[i % 64].wrlock();
				sum += pages[i].delhalf();
				rwlocks[i % 64].unwrlock();
			}
			this->size -= sum;
			return sum;
		}

		int docszie()
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
				fwriter.writeInt32(this->hashmode);
				for (uint i = 0; i < this->hashmode; i++)
				{
					rwlocks[i % 64].rdlock();
					pages[i].readall(fwriter);
					rwlocks[i % 64].unrdlock();
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
				int templength = 1024 * 1024;
				char* temp = new char[templength];
				int num = reader.readInt32();
				for (int i = 0; i < num; i++)
				{
					int segnum = reader.readInt32();
					for (int i = 0; i < segnum; i++)
					{
						int64 key = reader.readInt64();
						int length = reader.readInt32();
						if (length <= templength)
						{
							reader.read(temp, length);
						}
						else
						{
							templength = length;
							delete[] temp;
							temp = new char[templength];
							reader.read(temp, length);
						}
						rwlocks[_lock(key)].wrlock();
						if (pages[index(key)].insert(key, (signed char*) temp, length))
						{
							this->size++;
						}
						rwlocks[_lock(key)].unwrlock();
					}
				}
				delete[] temp;
			}
		}
	};
}
#endif
