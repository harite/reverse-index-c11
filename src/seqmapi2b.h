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
	const static int NODE_MAX_SIZE = 1024;
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

		void allocate(const char* ch, int pos, int length)
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
		page** pages{ nullptr };
		char* shardData;
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
			return index;
		}

		bool remove(uint index)
		{
			return this->pages[ypos(index)]->remove(index);
		}

		const char* find(uint index, int& length)
		{
			int _ypos = this->ypos(index);
			if (_ypos < this->pageLength)
			{
				return this->pages[_ypos]->find(index, length);
			}
			else
			{
				length = 0;
				return nullptr;
			}
		}
	};
	class i2b_block
	{
	private:
		seqblock _sblock;
		qstardb::seq64to32 seq;
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
			seq.create(key, index);
		    this->_sblock.insert(index,ch,length);
			return index;
		}

		int remove(int64 key)
		{
			uint index;
			if (this->seq.exists(key, index)) {
				this->_sblock.remove(index);
				this->seq.remove(key, index);
				return 1;
			}
			else {
				return 0;
			}

		}

		const char* find(int64 key, int& length)
		{
			uint index;
			if (this->seq.exists(key, index))
			{
				return 	this->_sblock.find(index, length);
			}
			else
			{
				length = 0;
				return nullptr;
			}
		}
	};
	class seqcache
	{
	private:
		int partition;
		i2b_block** blocks;
		qstardb::rwsyslock* rwlock;
		inline int _hash(int64 key)
		{
			int _h = key % this->partition;
			return _h > 0 ? _h : -_h;
		}
	public:
		seqcache(int partition)
		{
			this->partition = partition;
			this->blocks = new i2b_block*[this->partition];
			this->rwlock = new qstardb::rwsyslock[this->partition];
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
			delete[] this->rwlock;
			delete[] this->blocks;
		}

		void add(int64 key, const char* ch, int length)
		{
			int pos = _hash(key);
			rwlock[pos].wrlock();
			this->blocks[pos]->insert(key, ch, length);
			rwlock[pos].unwrlock();
		}

		void remove(int64 key)
		{
			int pos = _hash(key);
			rwlock[pos].wrlock();
			this->blocks[pos]->remove(key);
			rwlock[pos].unwrlock();
		}

		bool find(int64 key, qstardb::charwriter& writer)
		{
			int length;
			int pos = _hash(key);
			rwlock[pos].rdlock();
			const char* temp = this->blocks[pos]->find(key, length);
			if (temp != nullptr)
			{
				writer.writeInt(length);
				writer.write(temp, length);
				rwlock[pos].unrdlock();
				return true;
			}
			else
			{
				writer.writeInt(0);
				rwlock[pos].unrdlock();
				return false;
			}
		}
	};
}

#endif
