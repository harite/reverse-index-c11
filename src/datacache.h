#pragma once
#ifndef  DATACACHE_H_
#define  DATACACHE_H_
#include "string.h"
#include "sequence.h"

namespace cache {
	typedef unsigned int uint;
	typedef long long int64;

	const int  pagesize = 1024 * 2;
	
	inline uint xpos(uint index)
	{
		return index % pagesize;
	}
	inline uint ypos(uint index) 
	{
		return index / pagesize;
	}

	struct  node
	{
		int offset{0};
		int length{0};
	};

	class page
	{
		char* datas;
		int datalength;
		int delsize{0};
		int datasize{0};
		int   nodenum{0};
		node  nodes[pagesize];
	public:
		page()
		{
			this->datalength = 128 * pagesize;
			this->datas = new char[this->datalength];
		}
		~page()
		{
			delete[] this->datas;
		}
		void allocate(const char* ch, int pos, int length)
		{
			this->datalength = this->datasize - this->delsize + length * (pagesize - this->nodenum + 1);
			char* temp = new char[this->datalength];

			//reset datasize
			this->datasize = 0;
			//优化存储结构，重新调整数据结构
			for (int i = 0; i < pagesize; i++)
			{
				if (i == pos)
				{
					this->nodes[i].offset = this->datasize;
					this->nodes[i].length = length;
					memmove(temp + this->datasize, ch, sizeof(char) * this->nodes[i].length);
					this->datasize += length;
				}
				else if (this->nodes[i].length > 0)
				{
					memmove(temp + this->datasize, datas + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
					this->nodes[i].offset = this->datasize;
					this->datasize += this->nodes[i].length;
				}
				else
				{
					this->nodes[i].length = 0;
				}
			}
			delete[] datas;
			this->delsize = 0;
			this->datas = temp;
		}

		const char* get(uint index, int& length)
		{
			int _xpos = xpos(index);
			if (nodes[_xpos].length > 0)
			{
				length = nodes[_xpos].length;
				return this->datas + nodes[_xpos].offset;
			}
			else
			{
				length = 0;
				return nullptr;
			}
		}

		//插入输入，如果是update，返回false，如果是新增，则返回true
		inline bool add(uint index,const char* ch, int length)
		{
			int pos = xpos(index);
			bool update = this->nodes[pos].length > 0;
			//数据已经存在
			if (this->nodes[pos].length > 0)
			{
				//数据操作为update且更新的数据小于等于原数据，则可直接覆盖原数据
				if (this->nodes[pos].length >= length)
				{
					this->delsize += this->nodes[pos].length - length;
					this->nodes[pos].length = length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				//数据发生update，新数据要比原来的占用空间大，当预申请空间足够时候直接分配
				else if (this->datasize + length < this->datalength)
				{
					
					this->delsize += this->nodes[pos].length;
					this->nodes[pos].offset = this->datasize;
					this->nodes[pos].length = length;
					this->datasize += length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				// 数据发生update，新数据要比原来的占用空间大，当预申请空间不足，需要重新申请、整理内存
				else
				{
					this->allocate(ch, pos, length);
				}
			}
			//如果该条数据之前被删除过，还没有复用，且空间足够
			else if (-this->nodes[pos].length >= length)
			{

				this->nodenum++;
				this->delsize -= length;
				this->nodes[pos].length = length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			//内存充足，则直接分配
			else if (this->datasize + length < this->datalength)
			{
				this->nodenum++;
				this->nodes[pos].offset = this->datasize;
				this->nodes[pos].length = length;
				this->datasize += length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			// 空间不足，需要重新分配或者重新申请
			else
			{
				this->nodenum++;
				this->allocate(ch, pos, length);
			}

			if (this->nodenum == pagesize && this->delsize > 1024)
			{
				this->allocate(ch, -1, length);
			}
			if (update)
			{
				return 0;
			}
			else 
			{
				return 1;
			}
		}

		//删除数据，如果数据存在，则返回true，如果数据不存在活已经被删除，则直接返回false
		inline bool remove(uint index)
		{
			int _xpos = xpos(index);
			if (this->nodes[_xpos].length > 0)
			{
				this->delsize += this->nodes[_xpos].length;
				this->nodes[_xpos].length = -this->nodes[_xpos].length;
				this->nodenum--;
				return true;
			}
			else
			{
				return false;
			}
		}
	};
	class seqcache
	{
		
		int pagenum{0};
		page** pages{nullptr};

	public:
		seqcache()
		{
			this->pagenum = 1;
			this->pages = new page*[1];
			this->pages[0] = new page();
		}
		~seqcache()
		{
			for (int i = 0; i < pagenum; i++)
			{
				delete this->pages[i];
			}
			delete this->pages;
		}

		const char* get(uint index, int& length)
		{
			int _ypos = ypos(index);
			if (_ypos >= pagenum)
			{
				length = 0;
				return nullptr;
			}
			else
			{
				return pages[_ypos]->get(index,length);
			}
		}
		inline bool add(uint index,const char* doc, int& length)
		{
			int _ypos = ypos(index);
			if (_ypos >= pagenum)
			{
				if (_ypos > pagenum )
				{
					return false;
				}
				else
				{
					//扩容
					page**	temp = new page*[pagenum+1];
					memmove(temp , pages, sizeof(page*)*pagenum);
					delete[] this->pages;
					this->pages = temp;
					this->pagenum++;
					pages[_ypos]->add(index, doc, length);
					return true;
				}
			}
			else
			{
				 pages[_ypos]->add(index,doc,length);
				 return true;
			}

		}
		inline bool remove(uint index)
		{
			int _ypos = ypos(index);
			if (_ypos >= pagenum)
			{
				return false;
			}
			else
			{
				this->pages[_ypos]->remove(index);
				return true;
			}
		}

	};

	class kvcache
	{
		seqcache cache;
		qstardb::seq64to32 seqmap;
		

	public:
		kvcache() =default;
		~kvcache() = default;
		int add(int64 key,const char* ch,int length)
		{
			uint innerseq;
			bool insert=seqmap.create(key,innerseq);
			this->cache.add(innerseq,ch,length);
			if (insert) {
				//insert
				return 0;
			}
			else {
				//update
				return 1;
			}
		}

		bool remove(int64 key)
		{
			uint innerseq;
			if (seqmap.exists(key, innerseq))
			{
				
				return this->cache.remove(innerseq);
			}
			else 
			{
				return false;
			}
		}

		const char* get(int64 key,int& length)
		{
			uint innerseq;
			if (seqmap.exists(key, innerseq))
			{
				return this->cache.get(innerseq,length);
			}
			else 
			{
				return nullptr;
			}
		}
	};
}


#endif
