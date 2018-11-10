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

		int compare(int64 key)
		{
			if (this->key < key)
			{
				return -1;
			}
			else if (this->key > key)
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}
	};

	class page
	{
	private:
		char* mems;
		int delsize;
		int mensize;
		int menlength;

		entry* nodes;
		int nodesize;
		int nodelength;

		void ensureNodeCapacity(int length)
		{
			if (length > this->nodelength)
			{
				this->nodelength = length + 64;
				entry* temp = new entry[this->nodelength];
				memmove(temp, nodes, sizeof(entry) * this->nodesize);
				delete[] this->nodes;
				this->nodes = temp;
			}
		}
		void ensureDataCapacity(int length)
		{
			if (length > this->menlength)
			{
				this->menlength += length + (this->menlength >> 1) + 64;
				char* temp = new char[this->menlength];
				if (this->delsize > (length - this->menlength) && this->delsize * 3 > this->mensize)
				{
					int offset = 0;
					for (int i = 0; i < nodesize; i++)
					{

						memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * nodes[i].vlen);
						nodes[i].set(nodes[i].key, offset, nodes[i].vlen);
						offset += nodes[i].vlen;
					}
					this->delsize = 0;
					this->mensize = offset;
				}
				else
				{
					memmove(temp, mems, sizeof(char) * this->mensize);
				}
				delete[] this->mems;
				this->mems = temp;
			}
		}
	public:
		page()
		{
			this->delsize = 0;
			this->mensize = 0;
			this->menlength = 128;
			this->nodesize = 0;
			this->nodelength = 4;
			this->mems = new char[this->menlength];
			this->nodes = new entry[this->nodelength];
		}
		~page()
		{
			delete[] this->mems;
			delete[] this->nodes;
		}

		int indexof(int64 key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->nodesize - 1;
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
					return fromIndex > this->nodesize ? this->nodesize : fromIndex;
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
			node.set(key, this->mensize, vlen);
			memmove(mems + this->mensize, value, vlen);
			this->mensize += vlen;
		}

		bool insert(int64 key, signed char* value, short vlen)
		{
			ensureNodeCapacity(this->nodesize + 1);
			ensureDataCapacity(this->mensize + vlen);
			if (this->nodesize == 0 || nodes[this->nodesize - 1].compare(key) < 0)
			{
				this->set(nodes[this->nodesize], key, value, vlen);
				this->nodesize++;
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
						this->delsize += nodes[index].vlen - vlen;
						memmove(mems + nodes[index].offset, value, vlen);
						nodes[index].set(key, nodes[index].offset, vlen);
					}
					else
					{
						this->delsize += nodes[index].vlen;
						this->set(nodes[index], key, value, vlen);
					}
					return false;
				}
				else
				{
					int moveNum = this->nodesize - index;
					if (moveNum > 0)
					{
						memmove(nodes + index + 1, nodes + index, sizeof(entry) * moveNum);
					}
					this->set(nodes[index], key, value, vlen);
					this->nodesize++;
					return true;
				}
			}
		}

		bool remove(int64 key)
		{
			if (this->nodesize == 0)
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
					int moveNum = this->nodesize - index - 1;
					this->delsize -= this->nodes[index].vlen;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(entry) * moveNum);
					}
					this->nodesize--;
					if (this->nodelength > (this->nodesize + 3) * 3)
					{
						this->nodelength = (this->nodesize + 3) * 2;
						entry* temp = new entry[this->nodelength];
						memmove(temp, nodes, sizeof(entry) * this->nodesize);
						delete[] nodes;
						this->nodes = temp;
					}

					if (this->delsize > this->mensize * 3)
					{
						int offset = 0;
						this->menlength = this->mensize * 2;
						char* temp = new char[this->menlength];
						for (int i = 0; i < nodesize; i++)
						{
							int kvlength = nodes[i].vlen;
							memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * kvlength);
							nodes[i].set(key, offset, nodes[i].vlen);
							offset += kvlength;
						}
						this->delsize = 0;
						this->mensize = offset;
					}
					return true;
				}
			}
		}

		bool get(int64 key, string& word)
		{
			if (this->nodesize == 0)
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
			writer.writeInt32(this->nodesize);
			for (int i = 0; i < this->nodesize; i++)
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
