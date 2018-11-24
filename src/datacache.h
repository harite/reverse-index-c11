#pragma once
#ifndef  DATACACHE_H_
#define  DATACACHE_H_
#include "string.h"
#include "sequence.h"

namespace cache {
	typedef unsigned int uint;
	typedef long long int64;

	const uint  pagesize = 1024 * 2;
	
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
		int delsize;
		int datasize;
		int datalength;
		int   nodenum;
		node  nodes[pagesize];
	public:
		page()
		{
			this->delsize = 0;
			this->datasize = 0;
			this->nodenum = 0;
			this->datalength = 128 * pagesize;
			this->datas = new char[datalength];
		}
		~page()
		{
			delete[] this->datas;
		}
		void allocate(char* ch, int pos, int length)
		{
			int newlength = this->datasize - this->delsize + length * (pagesize - this->nodenum + 1);
			char* temp = new char[newlength];
			this->datasize = 0;
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
			this->datas = temp;
			this->delsize = 0;
			this->datalength = newlength;
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
		inline int insert(uint index, char* ch, int length)
		{
			int pos = xpos(index);
			//数据已经存在
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
				else if (this->datasize + length < this->datalength)
				{
					this->delsize += this->nodes[pos].length;
					this->nodes[pos].offset = this->datasize;
					this->nodes[pos].length = length;
					this->datasize += length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				// 该条数据为该页的最后一条数据
				else if (this->delsize >= length && this->nodenum == pagesize)//内存不足
				{
					this->allocate(ch, pos, length);
				}
				//标记删除的空间达到一半
				else if (this->delsize > length && this->delsize * 2 > this->datasize) {
					this->allocate(ch, pos, length);
				}
				else
				{
					this->allocate(ch, pos, length);
				}
			}
			else if (-this->nodes[pos].length >= length)
			{

				this->nodenum++;
				this->delsize -= length;
				this->nodes[pos].length = length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}      //内存充足，则直接分配
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
		
			return 1;
		}
		inline int remove(uint index)
		{
			int _xpos = xpos(index);
			if (this->nodes[_xpos].length > 0)
			{
				this->delsize += this->nodes[_xpos].length;
				this->nodes[_xpos].length = -this->nodes[_xpos].length;
				this->nodenum--;
				return 1;
			}
			else
			{
				return -1;
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
			for (size_t i = 0; i < pagenum; i++)
			{
				delete this->pages[i];
			}
			delete this->pages;
		}

		const char* get(uint index, int& length)
		{
			int _ypos = ypos(index);
			if (_ypos > pagenum)
			{
				length = 0;
				return nullptr;
			}
			else
			{
				return pages[_ypos]->get(index,length);
			}
		}
		inline int insert(uint index, char* doc, int& length)
		{
			int _ypos = ypos(index);
			if (_ypos >= pagenum)
			{
				if (_ypos > pagenum )
				{
					return -1;
				}
				else
				{
					//扩容
					page**	temp = new page*[pagenum+1];
					memmove(temp , pages, sizeof(page*)*pagenum);
					delete[] this->pages;
					this->pages = temp;
					this->pagenum++;
					return pages[_ypos]->insert(index, doc, length);
				}
			}
			else
			{
				return pages[_ypos]->insert(index,doc,length);
			}

		}
		inline int remove(uint index)
		{
			int _ypos = ypos(index);
			if (_ypos >= pagenum)
			{
				return -1;
			}
			else
			{
				return this->pages[_ypos]->remove(index);
			}
		}

	};

	class kvcache
	{
		qstardb::seq64to32 seqmap;
		seqcache cache;

	public:
		int insert(int64 key,char* ch,int length)
		{
			uint innerseq;
			bool insert=seqmap.create(key,innerseq);
			this->cache.insert(innerseq,ch,length);
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
				this->cache.remove(innerseq);
				return true;
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