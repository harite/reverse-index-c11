#pragma once
#ifndef _SEQMAP_H
#define _SEQMAP_H
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
	class page {
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
			if (this->nodes[xpos(index)].length <= 0) {
				length = 0;
				return nullptr;
			}
			else
			{
				length = this->nodes[xpos(index)].length;
				return datas + this->nodes[xpos(index)].offset;
			}
		}
		//释放了多少内存就返回多少大小
		bool remove(int index)
		{
			int oldsize = this->nodes[xpos(index)].length;
			if (oldsize > 0)
			{
				this->nodes[xpos(index)].length = -oldsize;
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
			//数据已经存在
			if (this->nodes[pos].length > 0)
			{

				//数据操作为update
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
				// 该条数据为该页的最后一条数据
				else if (this->delSize >= length && this->nodeSize == NODE_MAX_SIZE)//内存不足
				{
					this->allocate(ch, pos, length);
				}
				//标记删除的空间达到一半
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
			}      //内存充足，则直接分配
			else if (this->usedSize + length < this->dataLength)
			{
				this->nodeSize++;
				this->nodes[pos].offset = this->usedSize;
				this->nodes[pos].length = length;
				this->usedSize += length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			// 空间不足，需要重新分配或者重新申请
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
	class block
	{
	private:
		int nodeSize{ 0 };
		int pageLength{ 1 };
		page** pages{ nullptr };
		qstardb::seq64to32 seq;
		//复制临时空间
		char* shardData;
		inline int ypos(int index)
		{
			return index / NODE_MAX_SIZE;
		}
	public:
		block()
		{

			this->pages = new page*[pageLength];
			this->shardData = new char[SHARD_COPY_SIZE];
			this->pages[0] = new page(shardData);

		}

		~block()
		{
			for (int i = 0; i < pageLength; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
			delete[] this->shardData;
		}

		int insert(int64 key, const char* ch, int length)
		{
			uint index;
			seq.create(key, index);
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
			this->pages[ypos(index)]->insert(index, ch, length, optimize);
			return index;
		}

		int remove(int64 key)
		{
			uint index;
			if (this->seq.exists(key, index)) {
				this->pages[ypos(index)]->remove(index);
				this->seq.remove(key, index);
			}
		}

		const char* find(int64 key, int& length)
		{
			uint index;
			if (this->seq.exists(key, index))
			{
				return this->pages[ypos(index)]->find(index, length);
			}
			else
			{
				length;
				return nullptr;
			}
		}
	};
	class cache
	{
	private:
		int partition;
		block** blocks;
		qstardb::rwsyslock* rwlock;
		inline int _hash(int64 key)
		{
			int _h = key % this->partition;
			return _h > 0 ? _h : -_h;
		}
	public:
		cache(int partition)
		{
			this->partition = partition;
			this->blocks = new block*[this->partition];
			this->rwlock = new qstardb::rwsyslock[this->partition];
			for (int i = 0; i < this->partition; i++)
			{
				this->blocks[i] = new block();
			}
		}
		~cache()
		{
			for (int i = 0; i < this->partition; i++)
			{
				delete this->blocks[i];
			}
			delete[] this->rwlock;
			delete[] this->blocks;

		}

		void insert(int64 key, const char* ch, int length)
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
				rwlock[pos].unrdlock();
				return false;
			}
		}
	};
}

#endif