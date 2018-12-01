/*
 * dicmapi2s.h
 *  整形->字符串，主要用于存储字典表类型的小字符串键值对
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */

#ifndef DICMAPI2S_H_
#define DICMAPI2S_H_
#include <atomic>
#include <string.h>
#include "classdef.h"
#include "rwsyslock.h"
#include "filestream.h"
using namespace std;
namespace mapi2s
{
	typedef unsigned int uint;
	typedef long long int64;
	class entry
	{
	public:
		int64 key;
		int offset;
		short vlen;

		void set(int64 key, int offset, short vlen)
		{
			this->key = key;
			this->offset = offset;
			this->vlen = vlen;
		}
		inline int compare(int64 key)
		{
			return this->key > key ? 1 : (this->key < key ? -1 : 0);
		}
	};

	class page
	{
	private:
		char* mems;
		int delSize;
		int menSize;
		int menLength;

		entry* nodes;
		int nodeSize;
		int nodeLength;

		void ensureNodeCapacity(int length)
		{
			if (length > this->nodeLength)
			{
				this->nodeLength = length + 64;
				entry* temp = new entry[this->nodeLength];
				memmove(temp, nodes, sizeof(entry) * this->nodeSize);
				delete[] this->nodes;
				this->nodes = temp;
			}
		}
		void ensureDataCapacity(int length)
		{
			if (length > this->menLength)
			{
				this->menLength += length + (this->menLength >> 1) + 64;
				char* temp = new char[this->menLength];
				if (this->delSize > (length - this->menLength) && this->delSize * 3 > this->menSize)
				{
					int offset = 0;
					for (int i = 0; i < nodeSize; i++)
					{

						memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * nodes[i].vlen);
						nodes[i].set(nodes[i].key, offset, nodes[i].vlen);
						offset += nodes[i].vlen;
					}
					this->delSize = 0;
					this->menSize = offset;
				}
				else
				{
					memmove(temp, mems, sizeof(char) * this->menSize);
				}
				delete[] this->mems;
				this->mems = temp;
			}
		}
	public:
		page()
		{
			this->delSize = 0;
			this->menSize = 0;
			this->menLength = 128;
			this->nodeSize = 0;
			this->nodeLength = 4;
			this->mems = new char[this->menLength];
			this->nodes = new entry[this->nodeLength];
		}
		~page()
		{
			delete[] this->mems;
			delete[] this->nodes;
		}

		int indexof(int64 key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->nodeSize - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = nodes[mid].compare(key);
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == qstardb::type_insert ? -(mid + 1) : mid; // key
			}
			switch (_type)
			{
				case qstardb::type_insert:
					return fromIndex > this->nodeSize ? this->nodeSize : fromIndex;
				case qstardb::type_ceil:
					return fromIndex;
				case qstardb::type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}

		void set(entry& node, int64 key, signed char* value, short vlen)
		{
			node.set(key, this->menSize, vlen);
			memmove(mems + this->menSize, value, vlen);
			this->menSize += vlen;
		}

		bool insert(int64 key, signed char* value, short vlen)
		{
			ensureNodeCapacity(this->nodeSize + 1);
			ensureDataCapacity(this->menSize + vlen);
			if (this->nodeSize == 0 || nodes[this->nodeSize - 1].compare(key) < 0)
			{
				this->set(nodes[this->nodeSize], key, value, vlen);
				this->nodeSize++;
				return true;
			}
			else
			{
				int index = indexof(key, qstardb::type_insert);
				if (index < 0)
				{
					index = -index - 1;
					if (nodes[index].vlen >= vlen)
					{
						this->delSize += nodes[index].vlen - vlen;
						memmove(mems + nodes[index].offset, value, vlen);
						nodes[index].set(key, nodes[index].offset, vlen);
					}
					else
					{
						this->delSize += nodes[index].vlen;
						this->set(nodes[index], key, value, vlen);
					}
					return false;
				}
				else
				{
					int moveNum = this->nodeSize - index;
					if (moveNum > 0)
					{
						memmove(nodes + index + 1, nodes + index, sizeof(entry) * moveNum);
					}
					this->set(nodes[index], key, value, vlen);
					this->nodeSize++;
					return true;
				}
			}
		}

		bool remove(int64 key)
		{
			if (this->nodeSize == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(key, qstardb::type_index);
				if (index < 0)
				{
					return false;
				}
				else
				{
					int moveNum = this->nodeSize - index - 1;
					this->delSize -= this->nodes[index].vlen;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(entry) * moveNum);
					}
					this->nodeSize--;
					if (this->nodeLength > (this->nodeSize + 3) * 3)
					{
						this->nodeLength = (this->nodeSize + 3) * 2;
						entry* temp = new entry[this->nodeLength];
						memmove(temp, nodes, sizeof(entry) * this->nodeSize);
						delete[] nodes;
						this->nodes = temp;
					}

					if (this->delSize > this->menSize * 3)
					{
						int offset = 0;
						this->menLength = this->menSize * 2;
						char* temp = new char[this->menLength];
						for (int i = 0; i < nodeSize; i++)
						{
							int kvlength = nodes[i].vlen;
							memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * kvlength);
							nodes[i].set(key, offset, nodes[i].vlen);
							offset += kvlength;
						}
						this->delSize = 0;
						this->menSize = offset;
					}
					return true;
				}
			}
		}

		bool get(int64 key, string& word)
		{
			if (this->nodeSize == 0)
			{
				return false;
			}
			int index = indexof(key, qstardb::type_index);
			if (index >= 0)
			{
				const char* temp = mems + nodes[index].offset;
				word.append(temp, nodes[index].vlen);
				return true;
			}
			else
			{
				return false;
			}
		}

		void readall(qstardb::filewriter& writer)
		{
			writer.writeInt32(this->nodeSize);
			for (int i = 0; i < this->nodeSize; i++)
			{
				writer.writeInt64(nodes[i].key);
				writer.writeInt32(nodes[i].vlen);
				writer.writeBytes(mems + nodes[i].offset, nodes[i].vlen);
			}
		}
	};

	class dici2s
	{
	private:

		page* pages;
		uint hashmode;
		std::atomic<int> size { 0 };
		qstardb::rwsyslock rwlocks[64];
		unsigned int index(int64 key)
		{
			int value = key % hashmode;
			return value >= 0 ? value : -value;
		}
	public:
		dici2s(int hashmode)
		{
			this->size = 0;
			this->hashmode = hashmode;
			this->pages = new page[hashmode];
		}
		~dici2s()
		{
			delete[] this->pages;
		}

		int remove(int64 key)
		{
			uint pos = index(key);
			rwlocks[pos % 64].wrlock();
			if (pages[pos].remove(key))
			{
				this->size--;
			}
			rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		int insert(int64 key, signed char* value, short vlen)
		{
			uint pos = index(key);
			rwlocks[pos % 64].wrlock();
			if (pages[pos].insert(key, value, vlen))
			{
				this->size++;
			}
			rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		bool get(int64 key, string& word)
		{
			uint pos = index(key);
			rwlocks[pos % 64].rdlock();
			bool result = pages[pos].get(key, word);
			rwlocks[pos % 64].unrdlock();
			return result;
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
						if (pages[index(key)].insert(key, (signed char*) temp, length))
						{
							this->size++;
						}
					}
				}
				delete[] temp;
			}
		}
	};
}
#endif /* INTDICMAP_H_ */
