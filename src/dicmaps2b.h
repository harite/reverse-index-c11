/*
 * dicmaps2b.h
 *  字符串兼职对，用于存储键为字符串的数据
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef DICMAPS2B_H_
#define DICMAPS2B_H_
#include <string.h>
#include <atomic>
#include "filestream.h"
#include "classdef.h"
#include "rwsyslock.h"
using namespace std;
namespace maps2b
{
	typedef long long int64;
	typedef unsigned int uint;

	class entry
	{
	public:
		short klen { 0 };
		int vlen { 0 };
		char* ch { null };
		entry() = default;
		~entry() = default;

		void set(string& skey, signed char* value, int vlen, bool createNew)
		{
			const char* key = skey.c_str();
			short klen = skey.length();
			if (ch == null || createNew)
			{
				this->klen = klen;
				this->vlen = vlen;
				this->ch = new char[klen + vlen];
				memmove(ch, key, this->klen);
				memmove(ch + klen, value, this->vlen);
			}
			else if ((klen + vlen) <= (this->klen + this->vlen))
			{
				this->klen = klen;
				this->vlen = vlen;
				memmove(ch, key, this->klen);
				memmove(ch + klen, value, this->vlen);
			}
			else
			{
				this->free();
				this->klen = klen;
				this->vlen = vlen;
				this->ch = new char[klen + vlen];
				memmove(ch, key, this->klen);
				memmove(ch + klen, value, this->vlen);
			}
		}

		int compare(string& key)
		{
			const char* ckey = key.c_str();
			short klen = key.length();
			if (this->klen == klen)
			{
				for (int i = 0; i < klen; i++)
				{
					if (this->ch[i] == ckey[i])
					{
						continue;
					}
					else if (this->ch[i] > ckey[i])
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				return 0;
			}
			else
			{
				return this->klen - klen;
			}
		}

		const char* getValue()
		{
			return ch + klen;
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
		entry* nodes;

		int indexof(string& key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->size - 1;
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
					return fromIndex > this->size ? this->size : fromIndex;
				case qstardb::type_ceil:
					return fromIndex;
				case qstardb::type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}
	public:
		page()
		{
			this->size = 0;
			this->length = 8;
			this->nodes = new entry[8];
		}
		~page()
		{
			for (int i = 0; i < size; i++)
			{
				this->nodes[i].free();
			}
			delete[] this->nodes;
		}
		bool insert(string& key, signed char* ch, int vlen)
		{
			int index = indexof(key, qstardb::type_insert);
			if (index < 0)
			{
				index = -index - 1;
				nodes[index].set(key, ch, vlen, false);
				return false;
			}
			else
			{
				if (this->size == this->length)
				{
					this->length += 64 + (this->length >> 2);
					entry* temp = new entry[this->length];
					memmove(temp, nodes, sizeof(entry) * this->size);
					delete[] this->nodes;
					this->nodes = temp;
				}
				int moveNum = this->size - index;
				if (moveNum > 0)
				{
					memmove(nodes + index + 1, nodes + index, sizeof(entry) * moveNum);
				}

				this->nodes[index].set(key, ch, vlen, true);
				this->size++;
				return true;
			}
		}

		bool remove(string& key)
		{
			if (size == 0)
			{
				return false;
			}
			int index = indexof(key, qstardb::type_index);
			if (index < 0)
			{
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
				return true;
			}
		}

		const char* get(string& key, int& vlen)
		{
			if (size == 0)
			{
				vlen = 0;
				return null;
			}
			int index = indexof(key, qstardb::type_index);
			if (index >= 0)
			{
				vlen = nodes[index].vlen;
				const char* temp = nodes[index].getValue();
				return temp;
			}
			else
			{
				vlen = 0;
				return null;
			}
		}
		void readall(qstardb::filewriter& writer)
		{
			writer.writeInt32(this->size);
			for (int i = 0; i < this->size; i++)
			{
				writer.writeShort(nodes[i].klen);
				writer.writeBytes(nodes[i].ch, nodes[i].klen);
				writer.writeInt32(nodes[i].vlen);
				writer.writeBytes(nodes[i].ch + nodes[i].klen, nodes[i].vlen);
			}
		}
	};

	class dics2b
	{
	private:
		page* pages;
		uint hashmode;
		atomic<int> size{ 0 };
		qstardb::rwsyslock rwlocks[64];
		uint index(string& key)
		{
			return _hash(key) % hashmode;
		}
		inline uint _hash(string& key)
		{
			const char* str = key.c_str();
			int size = key.length();
			uint hash = 0;
			while (size-- > 0)
			{
				hash = hash * 31 + (*str++);
			}
			return hash;
		}

	public:
		dics2b(int hashmode)
		{
			this->hashmode = hashmode;
			this->pages = new page[hashmode];
		}
		~dics2b()
		{
			delete[] this->pages;
			delete[] this->rwlocks;
		}

		int remove(string& key)
		{
			uint pos = index(key);
			this->rwlocks[pos % 64].wrlock();
			if (pages[pos].remove(key))
			{
				this->size--;
			}
			this->rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		int insert(string& key, signed char* value, int vlen)
		{
			uint pos = index(key);
			this->rwlocks[pos % 64].wrlock();
			if (pages[pos].insert(key, value, vlen))
			{
				this->size++;
			}
			this->rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		const char* get(string& key, int& vlength)
		{
			uint pos = index(key);
			this->rwlocks[pos % 64].rdlock();
			const char* temp = pages[pos].get(key, vlength);
			this->rwlocks[pos % 64].unrdlock();
			return temp;

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
					this->rwlocks[i % 64].rdlock();
					pages[i].readall(fwriter);
					this->rwlocks[i % 64].unrdlock();
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
				char* tempkey = new char[64 * 1024];
				char* tempvalue = new char[templength];
				int num = reader.readInt32();
				for (int i = 0; i < num; i++)
				{
					int segnum = reader.readInt32();
					for (int i = 0; i < segnum; i++)
					{
						short klen = reader.readShort();
						reader.read(tempkey, klen);
						int length = reader.readInt32();
						if (length <= templength)
						{
							reader.read(tempvalue, length);
						}
						else
						{
							templength = length;
							delete[] tempvalue;
							tempvalue = new char[templength];
							reader.read(tempvalue, length);
						}
						string key(tempkey, klen);
						if (pages[index(key)].insert(key, (signed char*) tempvalue, length))
						{
							this->size++;
						}
					}
				}
				delete[] tempkey;
				delete[] tempvalue;
			}
		}
	};
}
#endif
