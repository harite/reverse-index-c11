/*
 * dicmapi2b.h
 *  
 *      add 2018-09-08
 *      Author: jkuang
 */
#pragma once
#ifndef _SEQMAP_I2B_H
#define _SEQMAP_I2B_H
#include<string.h>
#include<iostream>
#include "stdio.h"
#include "sequence.h"
#include "filestream.h"
using namespace std;
namespace seqmap {

	typedef long long int64;
	typedef unsigned int uint;
	const static int NODE_MAX_SIZE = 1024*4;
	const static int SHARD_COPY_SIZE = 1024 * 1024;
	class node
	{
	public:
		int offset{ 0 };
		int length{ 0 };
	};
	class page 
	{
		int usedSize{ 0 };
		int delSize{ 0 };
		int dataLength;

		char* datas{ nullptr };
		char* shardData;
		int nodeSize{ 0 };
		node nodes[NODE_MAX_SIZE];

		inline int xpos(int index)
		{
			return index % NODE_MAX_SIZE;
		}
	public:
		page(char* shardData)
		{
			this->shardData = shardData;
			this->dataLength = NODE_MAX_SIZE * 64;
			this->datas = new char[this->dataLength];
		}
		~page()
		{
			delete[] datas;
		}
		const char* find(int index, int& length)
		{
			int pos = xpos(index);
			if (this->nodes[pos].length <= 0)
			{
				length = 0;
				return nullptr;
			}
			else
			{
				length = this->nodes[pos].length;
				return datas + this->nodes[pos].offset;
			}
		}
		bool remove(int index)
		{
			int pos = xpos(index);
			int oldsize = this->nodes[pos].length;
			if (oldsize > 0)
			{
				this->nodes[pos].length = -oldsize;
				this->delSize += oldsize;
				this->nodeSize--;
				return true;
			}
			else
			{
				return false;
			}
		}

		inline void allocate(const char* ch, int pos, int length)
		{
			char* tempCopyData;
			bool useShardData = (this->delSize > length * 10) && (this->usedSize <= SHARD_COPY_SIZE);
			if (useShardData)
			{
				tempCopyData = shardData;
			}
			else
			{
				this->dataLength = this->usedSize - this->delSize + length * (NODE_MAX_SIZE - this->nodeSize + 1);
				tempCopyData = new char[this->dataLength];
			}
			this->usedSize = 0;
			this->delSize = 0;
			for (int i = 0; i < NODE_MAX_SIZE; i++)
			{
				if (i == pos)
				{
					this->nodes[i].offset = this->usedSize;
					this->nodes[i].length = length;
					memmove(tempCopyData + this->usedSize, ch, sizeof(char) * this->nodes[i].length);
					this->usedSize += length;
				}
				else if (this->nodes[i].length > 0)
				{
					memmove(tempCopyData + this->usedSize, datas + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
					this->nodes[i].offset = this->usedSize;
					this->usedSize += this->nodes[i].length;
				}
				else
				{
					this->nodes[i].length = 0;
				}
			}
			if (useShardData)
			{
				memmove(datas, tempCopyData, sizeof(char) * this->usedSize);
			}
			else
			{
				delete[] datas;
				this->datas = tempCopyData;
			}
		}

		void insert(int index, const char* ch, int length, bool optimize)
		{
			int pos = xpos(index);
			if (this->nodes[pos].length > 0)
			{
				if (this->nodes[pos].length >= length)
				{
					this->delSize += this->nodes[pos].length - length;
					this->nodes[pos].length = length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				else if (this->usedSize + length < this->dataLength)
				{
					this->delSize += this->nodes[pos].length;
					this->nodes[pos].offset = this->usedSize;
					this->nodes[pos].length = length;
					this->usedSize += length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				else if (this->delSize >= length && this->nodeSize == NODE_MAX_SIZE)//
				{
					this->allocate(ch, pos, length);
				}
				else if (this->delSize > length && this->delSize * 2 > this->usedSize) {
					this->allocate(ch, pos, length);
				}
				else
				{
					this->allocate(ch, pos, length);
				}
			}
			else if (-this->nodes[pos].length >= length)
			{

				this->nodeSize++;
				this->delSize -= length;
				this->nodes[pos].length = length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}      
			else if (this->usedSize + length < this->dataLength)
			{
				this->nodeSize++;
				this->nodes[pos].offset = this->usedSize;
				this->nodes[pos].length = length;
				this->usedSize += length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			else
			{
				this->nodeSize++;
				this->allocate(ch, pos, length);
			}
			if (this->nodeSize == NODE_MAX_SIZE && this->delSize > 1024)
			{
				this->allocate(ch, -1, length);
			}
		}

	};
	class seqblock
	{
	private:
		int nodeSize{ 0 };
		int pageLength{ 1 };

		page** pages;
		char* shardData;
		qstardb::rwsyslock lock;
		inline int ypos(int index)
		{
			return index / NODE_MAX_SIZE;
		}
	public:
		seqblock()
		{
			this->pages = new page*[pageLength];
			this->shardData = new char[SHARD_COPY_SIZE];
			this->pages[0] = new page(shardData);
		}

		~seqblock()
		{
			for (int i = 0; i < pageLength; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
			delete[] this->shardData;
		}

		int insert(uint index, const char* ch, int length)
		{
			lock.wrlock();
			int _ypos = this->ypos(index);
			if (_ypos >= this->pageLength)
			{
				page** temp = new page*[this->pageLength + 1];
				memmove(temp, this->pages, sizeof(page*)*this->pageLength);
				this->pageLength++;
				delete[] this->pages;
				this->pages = temp;
				this->pages[_ypos] = new page(shardData);
			}
			bool optimize = (index  % NODE_MAX_SIZE) == NODE_MAX_SIZE - 1;
			this->pages[_ypos]->insert(index, ch, length, optimize);
			lock.unwrlock();
			return index;
		}

		bool remove(uint index)
		{
			lock.wrlock();
			bool result= this->pages[ypos(index)]->remove(index);
			lock.unwrlock();
			return result;
		}

		bool find(uint index, string& str)
		{
		
			int _ypos = this->ypos(index);
			bool reuslt = false;
			lock.rdlock();
			if (_ypos < this->pageLength)
			{
				int length;
				const char* temp= this->pages[_ypos]->find(index, length);
				if (temp != nullptr) 
				{
					str.append(temp,length);
					reuslt = true;
				}
			}
			lock.unrdlock();
			return reuslt;
		}

		bool find(uint index, qstardb::charwriter& writer)
		{

			int _ypos = this->ypos(index);
			bool reuslt = false;
			lock.rdlock();
			if (_ypos < this->pageLength)
			{
				int length;
				const char* temp = this->pages[_ypos]->find(index, length);
				if (temp != nullptr)
				{
					writer.writeInt(length);
					writer.write(temp,length);
					reuslt = true;
				}
			}
			lock.unrdlock();
			return reuslt;
		}
		const char* find(uint index, int& length)
		{

			int _ypos = this->ypos(index);
			if (_ypos < this->pageLength)
			{
				return this->pages[_ypos]->find(index, length);
			}
			return nullptr;
		}
	};
	class i2b_block
	{
	private:
		seqblock _sblock;
		qstardb::seq64to32 seq;
		qstardb::rwsyslock lock;
		inline int ypos(int index)
		{
			return index / NODE_MAX_SIZE;
		}
	public:
		i2b_block() = default;

		~i2b_block() = default;

		int insert(int64 key, const char* ch, int length)
		{
			uint index;
			lock.wrlock();
			seq.create(key, index);
		    this->_sblock.insert(index,ch,length);
			lock.unwrlock();
			return index;
		}

		bool remove(int64 key)
		{
			uint index;
			lock.wrlock();
			if (this->seq.exists(key, index)) {
				
				this->_sblock.remove(index);
				this->seq.remove(key, index);
				lock.unwrlock();
				return true;
			}
			else {
				lock.unwrlock();
				return false;
			}

		}

		bool find(int64 key, string& word)
		{
			uint index;
			bool result = false;
			lock.rdlock();
			if (this->seq.exists(key, index))
			{
				result= this->_sblock.find(index,  word);
			}
			lock.unrdlock();
			return result;
		}

		bool find(int64 key, qstardb::charwriter& writer)
		{
			uint index;
			bool result = false;
			lock.rdlock();
			if (this->seq.exists(key, index))
			{
				result = this->_sblock.find(index, writer);
			}
			lock.unrdlock();
			return result;
		}
	};
	/**建议分区不超过十六个分区，否则每个分区都会消耗共享拷贝内存空间*/
	class seqcache
	{
	private:
		int partition;
		i2b_block** blocks;
		inline int _hash(int64 key)
		{
			int _h = key % this->partition;
			return _h > 0 ? _h : -_h;
		}
	public:
		seqcache(int partition)
		{
			if (partition < 1)
			{
				partition = 1;
			}
			else if (partition > 16)
			{
				partition = 16;
			}
			this->partition = partition;
			this->blocks = new i2b_block*[this->partition];
			for (int i = 0; i < this->partition; i++)
			{
				this->blocks[i] = new i2b_block();
			}
		}
		~seqcache()
		{
			for (int i = 0; i < this->partition; i++)
			{
				delete this->blocks[i];
			}
			delete[] this->blocks;
		}

		void add(int64 key, const char* ch, int length)
		{
			this->blocks[_hash(key)]->insert(key, ch, length);
		}

		void remove(int64 key)
		{
			this->blocks[_hash(key)]->remove(key);
		}

		bool find(int64 key, qstardb::charwriter& writer)
		{
			return this->blocks[_hash(key)]->find(key, writer);
		}

		bool get(int64 key, string& word)
		{
			return this->blocks[_hash(key)]->find(key, word);
		}
	};
}

#endif
