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
		void allocate(char* ch, int pos, int length)
		{
			this->datalength = this->datasize - this->delsize + length * (pagesize - this->nodenum + 1);
			char* temp = new char[this->datalength];

			//reset datasize
			this->datasize = 0;
			//�Ż��洢�ṹ�����µ������ݽṹ
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

		//�������룬�����update������false��������������򷵻�true
		inline bool add(uint index, char* ch, int length)
		{
			int pos = xpos(index);
			bool update = this->nodes[pos].length > 0;
			//�����Ѿ�����
			if (this->nodes[pos].length > 0)
			{
				//���ݲ���Ϊupdate�Ҹ��µ�����С�ڵ���ԭ���ݣ����ֱ�Ӹ���ԭ����
				if (this->nodes[pos].length >= length)
				{
					this->delsize += this->nodes[pos].length - length;
					this->nodes[pos].length = length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				//���ݷ���update��������Ҫ��ԭ����ռ�ÿռ�󣬵�Ԥ����ռ��㹻ʱ��ֱ�ӷ���
				else if (this->datasize + length < this->datalength)
				{
					
					this->delsize += this->nodes[pos].length;
					this->nodes[pos].offset = this->datasize;
					this->nodes[pos].length = length;
					this->datasize += length;
					memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
				}
				// ���ݷ���update��������Ҫ��ԭ����ռ�ÿռ�󣬵�Ԥ����ռ䲻�㣬��Ҫ�������롢�����ڴ�
				else
				{
					this->allocate(ch, pos, length);
				}
			}
			//�����������֮ǰ��ɾ��������û�и��ã��ҿռ��㹻
			else if (-this->nodes[pos].length >= length)
			{

				this->nodenum++;
				this->delsize -= length;
				this->nodes[pos].length = length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			//�ڴ���㣬��ֱ�ӷ���
			else if (this->datasize + length < this->datalength)
			{
				this->nodenum++;
				this->nodes[pos].offset = this->datasize;
				this->nodes[pos].length = length;
				this->datasize += length;
				memmove(datas + this->nodes[pos].offset, ch, sizeof(char) * length);
			}
			// �ռ䲻�㣬��Ҫ���·��������������
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

		//ɾ�����ݣ�������ݴ��ڣ��򷵻�true��������ݲ����ڻ��Ѿ���ɾ������ֱ�ӷ���false
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
			for (size_t i = 0; i < pagenum; i++)
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
		inline bool add(uint index, char* doc, int& length)
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
					//����
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
		int add(int64 key,char* ch,int length)
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