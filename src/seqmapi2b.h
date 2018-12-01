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
	const static int maxsize = 1024;
	class node
	{
	public:
		int offset{ 0 };
		int length{ 0 };
	};
	class page {
		int size{ 0 };
		int datalength;
		int delsize{ 0 };
		char* datas{ nullptr };
		node nodes[maxsize];
		int nodesize{ 0 };
		inline int xpos(int index)
		{
			return index % maxsize;
		}
	public:
		page()
		{
			this->datalength = maxsize * 64;
			this->datas = new char[this->datalength];
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
				this->delsize += oldsize;
				this->nodesize--;
				return true;
			}
			else
			{
				return false;
			}
		}
		void allocate(const char* ch, int pos, int length)
		{
			int newlength = this->size - this->delsize + length * (maxsize - this->nodesize + 1);
			char* temp = new char[newlength];
			this->size = 0;
			for (int i = 0; i < maxsize; i++)
			{
				if (i == pos)
				{
					this->nodes[i].offset = this->size;
					this->nodes[i].length = length;
					memmove(temp + this->size, ch, sizeof(char) * this->nodes[i].length);
					this->size += length;
				}
				else if (this->nodes[i].length > 0)
				{
					memmove(temp + this->size, datas + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
					this->nodes[i].offset = this->size;
					this->size += this->nodes[i].length;
				}
				else
				{
					this->nodes[i].length = 0;
				}
			}
			delete[] datas;
			this->datas = temp;
			this->delsize = 0;
			this->datalength = newlength;
		}

		void insert(int index,const char* ch, int length, bool optimize)
		{
			int pos = xpos(index);
			//数据已经存在
			if (this->nodes[pos].length > 0)
			{

				//数据操作为update
				if (this->nodes[pos].length >= length)
				{
					this->delsize += this->nodes[pos].length - length;
					this->nodes[pos].length = length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				else if (this->size + length < this->datalength)
				{
					this->delsize += this->nodes[pos].length;
					this->nodes[pos].offset = this->size;
					this->nodes[pos].length = length;
					this->size += length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				// 该条数据为该页的最后一条数据
				else if (this->delsize >= length && this->nodesize == maxsize)//内存不足
				{
					this->allocate(ch, pos, length);
				}
				//标记删除的空间达到一半
				else if (this->delsize > length && this->delsize * 2 > this->size) {
					this->allocate(ch, pos, length);
				}
				else
				{
					this->allocate(ch, pos, length);
				}
			}
			else if (-this->nodes[pos].length >= length)
			{

				this->nodesize++;
				this->delsize -= length;
				this->nodes[pos].length = length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}      //内存充足，则直接分配
			else if (this->size + length < this->datalength)
			{
				this->nodesize++;
				this->nodes[pos].offset = this->size;
				this->nodes[pos].length = length;
				this->size += length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			// 空间不足，需要重新分配或者重新申请
			else
			{
				this->nodesize++;
				this->allocate(ch, pos, length);
			}
			if (this->nodesize == maxsize && this->delsize > 1024)
			{
				this->allocate(ch, -1, length);
			}
		}

	};
	class block
	{
	private:
		page** pages{ nullptr };
		int nodeSize{ 0 };
		int pageLength{ 1 };
		qstardb::seq64to32 seq;
		inline int ypos(int index)
		{
			return index / maxsize;
		}
	public:
		block()
		{
			this->pages = new page*[pageLength];
			this->pages[0] = new page();
		}

		~block()
		{
			for (int i = 0; i < pageLength; i++)
			{
				delete pages[i];
			}
			delete[] pages;
		}

		int insert(int64 key,const char* ch, int length)
		{
			uint index;
			seq.create(key, index);
			int _ypos = this->ypos(index);
			if (_ypos >= this->pageLength)
			{
				page** temp = new page*[this->pageLength+1];
				memmove(temp,this->pages,sizeof(page*)*this->pageLength);
				this->pageLength++;
				delete[] pages;
				this->pages = temp;
				this->pages[_ypos] = new page();
			}
			bool optimize = (index  % maxsize) == maxsize - 1;
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