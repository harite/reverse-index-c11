/*
 * dicmaps2s.h
 *  字符串键值对，主要用于存储字典表数据
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef DICMAPS2S_H_
#define DICMAPS2S_H_
#include <string.h>
#include <string>
#include <atomic>
#include "rwsyslock.h"
#include "filestream.h"
using namespace std;
namespace maps2s
{
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};

	class entry
	{
	public:
		int offset { 0 };
		short klen { 0 };
		int vlen { 0 };
		entry() = default;
		~entry() = default;

		void set(int offset, short klen, int vlen)
		{
			this->offset = offset;
			this->klen = klen;
			this->vlen = vlen;
		}

		inline int compare(char* chs, const char* key, short klen)
		{
			if (this->klen == klen)
			{
				for (int i = 0; i < klen; i++)
				{
					if (chs[offset + i] == key[i])
					{
						continue;
					}
					else if (chs[offset + i] > key[i])
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
		void ensurenodecapacity(int length)
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
		void ensuredatacapacity(int length)
		{
			if (length > this->menLength)
			{
				this->menLength = length + (this->menLength / (nodeSize + 1)) * 32 + 64;
				char* temp = new char[this->menLength];
				if (this->delSize > (length - this->menLength) && this->delSize * 3 > this->menSize)
				{
					int offset = 0;
					for (int i = 0; i < nodeSize; i++)
					{
						int kvlength = nodes[i].klen + nodes[i].vlen;
						memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * kvlength);
						nodes[i].set(offset, nodes[i].klen, nodes[i].vlen);
						offset += kvlength;
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

		int indexof(const char* key, short klen, type _type)
		{
			int fromIndex = 0;
			int toIndex = this->nodeSize - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = nodes[mid].compare(mems, key, klen);
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
					return fromIndex > this->nodeSize ? this->nodeSize : fromIndex;
				case type_ceil:
					return fromIndex;
				case type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}

		void set(entry& node, const char* key, short klen, const char* value, int vlen)
		{
			node.set(this->menSize, klen, vlen);
			memmove(mems + this->menSize, key, klen);
			this->menSize += klen;
			memmove(mems + this->menSize, value, vlen);
			this->menSize += vlen;
		}

		bool insert(const char* key, short klen, const char* value, int vlen)
		{
			ensurenodecapacity(this->nodeSize + 1);
			ensuredatacapacity(this->menSize + klen + vlen);
			if (this->nodeSize == 0 || nodes[this->nodeSize - 1].compare(mems, key, klen) < 0)
			{
				this->set(nodes[this->nodeSize], key, klen, value, vlen);
				this->nodeSize++;
				return true;
			}
			else
			{
				int index = indexof(key, klen, type_insert);
				if (index < 0)
				{
					index = -index - 1;
					if (nodes[index].vlen >= vlen)
					{
						this->delSize += nodes[index].vlen - vlen;
						memmove(mems + nodes[index].offset + nodes[index].klen, value, vlen);
						nodes[index].vlen = vlen;
					}
					else
					{
						this->delSize += klen + nodes[index].vlen;
						this->set(nodes[index], key, klen, value, vlen);
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
					this->set(nodes[index], key, klen, value, vlen);
					this->nodeSize++;
					return true;
				}
			}
		}

		bool remove(const char* key, short klen)
		{
			if (this->nodeSize == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(key, klen, type_index);
				if (index < 0)
				{
					return false;
				}
				else
				{
					int moveNum = this->nodeSize - index - 1;
					this->delSize -= this->nodes[index].klen + this->nodes[index].vlen;
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
							int kvlength = nodes[i].klen + nodes[i].vlen;
							memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * kvlength);
							nodes[i].set(offset, nodes[i].klen, nodes[i].vlen);
							offset += kvlength;
						}
						this->delSize = 0;
						this->menSize = offset;
					}
					return true;
				}
			}
		}

		char* get(const char* key, short klen, int& vlen)
		{
			if (this->nodeSize == 0)
			{
				vlen = 0;
				return null;
			}
			int index = indexof(key, klen, type_index);
			if (index >= 0)
			{
				vlen = nodes[index].vlen;
				const char* temp = mems + nodes[index].offset + nodes[index].klen;
				char* result = new char[vlen];
				memcpy(result, temp, sizeof(char) * vlen);
				return result;
			}
			else
			{
				vlen = 0;
				return null;
			}
		}

		bool get(const char* key, short klen, string& word)
		{
			if (this->nodeSize == 0)
			{
				return false;
			}
			int index = indexof(key, klen, type_index);
			if (index >= 0)
			{
				// nodes[index].vlen;
				const char* temp = mems + nodes[index].offset + nodes[index].klen;
				word.append(temp, nodes[index].vlen);
				return true;
			}
			else
			{
				return false;
			}
		}

		int get(const char* key, short klen, qstardb::charwriter& writer)
		{
			int hits = 0;
			if (this->nodeSize == 0)
			{
				return hits;
			}
			int index = indexof(key, klen, type_index);
			if (index >= 0)
			{
				hits++;
				writer.writeShort(nodes[index].klen);
				writer.write(mems, nodes[index].offset, nodes[index].klen);
				writer.writeShort(nodes[index].vlen);
				writer.write(mems, nodes[index].offset + nodes[index].klen, nodes[index].vlen);
			}
			return hits;
		}
		void readall(qstardb::filewriter& writer)
		{
			writer.writeInt32(this->nodeSize);
			for (int i = 0; i < this->nodeSize; i++)
			{
				writer.writeShort(nodes[i].klen);
				writer.write(mems, nodes[i].offset, nodes[i].klen);
				writer.writeInt32(nodes[i].vlen);
				writer.write(mems, nodes[i].offset + nodes[i].klen, nodes[i].vlen);
			}
		}

	};

	class dics2s
	{
	private:
		typedef unsigned int uint;
		typedef long long int64;

		page* pages;
		uint hashmode;
		std::atomic<int> size { 0 };
		qstardb::rwsyslock rwlocks[64];

		inline uint _hash(const char* str, short len)
		{
			uint hash = 0;
			while (len-- > 0)
			{
				hash = hash * 31 + (*str++);
			}
			return hash;
		}

		unsigned int index(const char* key, short len)
		{
			return _hash(key, len) % hashmode;
		}
	public:
		dics2s(int hashmode)
		{
			this->size = 0;
			this->hashmode = hashmode > 0 ? hashmode : 16;
			this->pages = new page[this->hashmode];
		}
		~dics2s()
		{
			delete[] this->pages;
		}

		int remove(const char* key, short klen)
		{
			uint pos = index(key, klen);
			rwlocks[pos % 64].wrlock();
			if (pages[pos].remove(key, klen))
			{
				this->size--;
			}
			rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		int insert(const char* key, short klen, const char* value, int vlen)
		{
			uint pos = index(key, klen);
			rwlocks[pos % 64].wrlock();
			if (pages[pos % 64].insert(key, klen, value, vlen))
			{
				this->size++;
			}
			rwlocks[pos % 64].unwrlock();
			return this->size;
		}

		char* get(const char* key, short klen, int& vlen)
		{
			uint pos = index(key, klen);
			rwlocks[pos % 64].rdlock();
			char* ch = pages[pos].get(key, klen, vlen);
			rwlocks[pos % 64].unrdlock();
			return ch;
		}

		bool get(const char* key, short klen, string& word)
		{
			uint pos = index(key, klen);
			rwlocks[pos % 64].rdlock();
			bool result = pages[pos].get(key, klen, word);
			rwlocks[pos % 64].unrdlock();
			return result;
		}

		short readshort(const char* ch, int offset)
		{
			return (((short) ch[offset]) << 8) | (short) (ch[offset + 1] & 0xff);
		}

		int get(const char* keys, int length, qstardb::charwriter& writer)
		{
			int hits = 0;
			for (int offset = 0; offset < length;)
			{
				short klen = readshort(keys, offset);
				offset += 2;
				uint pos = index(keys + offset, klen);
				rwlocks[pos % 64].rdlock();
				if (pages[pos].get(keys + offset, klen, writer))
				{
					hits++;
				}
				rwlocks[pos % 64].unrdlock();
				offset += klen;
			}
			return hits;
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
						const char* key = tempkey;
						const char* value = tempvalue;

						if (pages[index(key, klen)].insert(key, klen, value, length))
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
