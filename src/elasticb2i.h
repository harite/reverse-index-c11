#pragma once
#ifndef _SEQMAP_B2I_H
#define _SEQMAP_B2I_H
#include "sequence.h"
#include "elastici2b.h"
//add 2018-09-08
namespace seqmap 
{
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};
	inline uint strhash(const char* str, int size)
	{
		uint hash = 0;
		while (size-- > 0)
		{
			hash = hash * 31 + (*str++);
		}
		return hash;
	}

	inline int compare(const char* ch0, int len0, const char* ch1, int len1)
	{
		if (len0 == len1)
		{
			for (int i = 0; i < len1; i++)
			{
				if (ch0[i] > ch1[i])
				{
					return 1;
				}
				else if (ch0[i] < ch1[i])
				{
					return -1;
				}
			}
			return 0;
		}
		else
		{
			return len0 - len1;
		}
	}
	class b2ipage
	{
	public:
		int size{0};
		int length;
		uint* keys;
		seqblock* _block;
		qstardb::sequence* seq;
		inline int indexof(uint* keys, int size, const char* key, int len,  type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int tempLength = 0;
				const char* temp = _block->find(keys[mid],tempLength);
				int cmp = compare(temp, tempLength, key, len);
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
																	// found
			}
			switch (_type)
			{
			case type_insert:
				return fromIndex > size ? size : fromIndex;
			case type_ceil:
				return fromIndex;
			case type_index:
				return -(fromIndex + 1);
			default:
				return toIndex;
			}
		}
	public:
		b2ipage( qstardb::sequence* seq, int len, seqblock* _block)
		{
			this->seq = seq;
			this->length = len;
			this->_block = _block;
			this->keys = new uint[this->length];
		}
		~b2ipage() 
		{
			delete[] keys;
		}
		int keySize()
		{
			return this->size;
		}
		int insert(const char* ch,int len)
		{
			if (this->size==0)
			{
				int index=this->seq->createsq();
				this->keys[this->size] = index;
				this->_block->insert(index,ch,len);
				this->size++;
				return index;
			}
			int pos = indexof(this->keys,this->size,ch,len,type_insert);
			if (pos >= 0)
			{
				if (this->size == this->length)
				{
					int extSize = this->length >> 1;
					this->length += extSize > 1024 ? 1024 : extSize;
					uint* temp = new uint[this->length];
					memmove(temp, this->keys, sizeof(int)*this->size);
					delete[] this->keys;
					this->keys = temp;
				}
				int numMoved = this->size - pos;
				if (numMoved > 0)
				{
					memmove(this->keys + pos + 1, this->keys + pos, sizeof(uint) * numMoved);
				}
				int index = this->seq->createsq();
				this->keys[pos] = index;
				this->_block->insert(index, ch, len);
				this->size++;
				return index;
			}
			else
			{
				return this->keys[-pos - 1];
			}
		}

		int get(const char* ch,int len)
		{
			if (this->size == 0)
			{
				return qstardb::MIN_INT_VALUE;
			}
			else 
			{
				int pos = indexof(this->keys, this->size, ch, len, type_index);
				if (pos >= 0)
				{
					return this->keys[pos];
				}
				else
				{
					return qstardb::MIN_INT_VALUE;
				}
			}

		}

		bool remove(const char* ch,int len)
		{
			if (this->size == 0)
			{
				return false;
			}
			else {
				int pos = indexof(this->keys, this->size, ch, len, type_index);
				if (pos >= 0) {
					int index = this->keys[pos];
					int numMoved = size - pos - 1;
					if (numMoved > 0)
					{
						memmove(this->keys + pos, this->keys + pos + 1, sizeof(uint) * numMoved);
					}
					//标记数据被删除
					this->_block->remove(index);
					//回收该分配的id
					this->seq->recycle(index);
					this->size--;
					return true;
				}
				else {
					return false;
				}
				
			}

		}

		/*数据库分裂为两个块*/
		b2ipage** splitToTwo(bool tail)
		{
			b2ipage** pages = new b2ipage*[2];
			int mid = (this->size * (tail ? 7 : 5)) / 10;
			pages[0] = new b2ipage(this->seq, mid+128,this->_block);
			pages[0]->size = mid;
			memmove(pages[0]->keys,this->keys,sizeof(int)*pages[0]->size);
		
			pages[1] = new b2ipage(this->seq, this->size - mid + 128, this->_block);
			pages[1]->size = this->size - mid;
			memmove(pages[1]->keys, this->keys+ mid, sizeof(int)*pages[1]->size);
			return pages;
		}

		/*判定页的范围是否包含 key*/
		int rangecontains(const char* key,int len)
		{
			if (this->size == 0)
			{
				return 0;
			}
			int tailLength = 0;
			const char* tail = _block->find(keys[this->size-1], tailLength);
			if (compare(tail, tailLength, key, len) < 0)
			{
				return -1;
			}
			int headLength = 0;
			const char* head = _block->find(keys[0], headLength);
			if (compare(head, headLength, key,len) > 0)
			{
				return 1;
			}
			return 0;
		}
	};

	class b2iblcok
	{
	private:
		int size{1};
		b2ipage** pages;
		seqblock* _block;
		qstardb::sequence* seq;
		inline int indexOf(b2ipage** pages, int size,const char* key,int len, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = pages[mid]->rangecontains(key,len);
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
				return fromIndex > size ? size : fromIndex;
			case type_ceil:
				return fromIndex;
			case type_index:
				return -(fromIndex + 1);
			default:
				return toIndex;
			}
		}
	public:
		b2iblcok(qstardb::sequence* seq, seqblock* _block)
		{
			this->seq = seq;
			this->_block = _block;
			this->pages = new b2ipage*[this->size];
			this->pages[0] = new b2ipage(this->seq,1024,this->_block);
		}
		~b2iblcok()
		{
			for (int i = 0; i < this->size; i++)
			{
				delete this->pages[i];
			}
			delete[] pages;
		}

		inline int insertAndSplit(int index,  const char* key, int length)
		{

			int reuslt = this->pages[index]->insert(key, length);
			if (this->pages[index]->size > 1024*8)
			{
				b2ipage** temp = this->pages[index]->splitToTwo(index == this->size - 1);
				b2ipage** newpages = new b2ipage*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(b2ipage*)*index);
				}
				int moveNum = this->size - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(page*) * moveNum);
				}
				newpages[index] = temp[0];
				newpages[index + 1] = temp[1];
				delete pages[index];
				delete[] temp;
				delete[] this->pages;
				this->pages = newpages;
				this->size++;
			}
			return reuslt;
		}

		bool combine(int index0, int index1)
		{
			b2ipage* temp0 = this->pages[index0];
			b2ipage* temp1 = this->pages[index1];
			int size0 = temp0->keySize();
			int size1 = temp1->keySize();
			b2ipage* newpage = new b2ipage(this->seq,size0+size1+128,this->_block);
			memmove(newpage->keys, temp0->keys, sizeof(int)*size0);
			memmove(newpage->keys+ size0, temp1->keys, sizeof(int)*size1);
			//设置新也的数据量
			newpage->size = size0 + size1;
			//释放原数据页1
			delete temp0;
			//释放原数据页2
			delete temp1;
			//将新页数据放到 index0的位置
			this->pages[index0] = newpage;
			//将空白页删除
			int moveNum = this->size - index1 - 1;
			if (moveNum > 0)
			{
				memmove(this->pages + index1, this->pages + index1 + 1, sizeof(b2ipage*)*moveNum);
			}
			this->size--;
			return true;
		}

		inline bool combine(int index)
		{
			if (this->size == 1)
			{
				return false;
			}
			//如果数据页内的数据小于64条，则合并数据页
			else if (this->pages[index]->keySize() <  64)
			{
				//如果是第一页，则将第二页的合并到第一页
				if (index == 0)
				{
					combine(0, 1);
				}
				else//其他情况则将数据将与前一页合并
				{
					combine(index - 1, index);
				}
				return true;
			}
			else
			{
				return false;
			}
		}

		int insert(const char* key, int len)
		{
			if (this->size == 1 || this->pages[0]->rangecontains(key,len) >= 0)
			{
				return this->insertAndSplit(0,key,len);
			}
			else if (this->pages[this->size - 1]->rangecontains(key,len) <= 0)
			{
				return this->insertAndSplit(this->size - 1, key, len);
			}
			else
			{
				int pageno = indexOf(pages, this->size,key,len,type_ceil);
				return this->insertAndSplit(pageno, key, len);
			}
		}

		bool remove(const char* key,int len)
		{
			if (this->pages[this->size - 1]->rangecontains(key,len) == 0)
			{
				bool deled = this->pages[this->size - 1]->remove(key,len);
				this->combine(this->size - 1);
				return deled;
			}
			else
			{
				int pageno = indexOf(this->pages, this->size,key,len, type_index);
				if (pageno >= 0)
				{
					bool deled = this->pages[pageno]->remove(key,len);
					this->combine(pageno);
					return deled;
				}
				return false;
			}
		}
		int get(const char* key,int len)
		{
			int pageno = indexOf(this->pages,  this->size,key,len, type_index);
			if (pageno >= 0) {
				return this->pages[pageno]->get(key, len);
			}
			else {
				return qstardb::MIN_INT_VALUE;
			}
		}
	};

	class b2imap
	{
	private:
		int partition;
		seqblock* _block;
		b2iblcok** blocks;
		qstardb::sequence* seq;
		qstardb::rwsyslock lock;
		inline int ypos(const char* ch,int len)
		{
			int pos =strhash(ch,len)% partition;
			if (pos < 0) {
				return -pos;
			}
			else {
				return pos;
			}
		}
	public:
		b2imap(int part)
		{
			this->partition = part;
			this->_block = new seqblock();
			this->seq = new qstardb::sequence();
			this->blocks = new b2iblcok*[part];
			for (int i = 0; i < part; i++)
			{
				this->blocks[i] = new b2iblcok(this->seq,this->_block);
			}
		}
		~b2imap()
		{
			for (int i = 0; i < this->partition; i++)
			{
				delete this->blocks[i];
			}
			delete[] this->blocks;
			delete this->seq;
			delete this->_block;
		}
		int curSeq()
		{
			return this->seq->get();
		}
		bool get(uint index,string& str)
		{
			bool result = false;
			lock.rdlock();
			result = this->_block->find(index, str);
			lock.unrdlock();
			return result;

		}
		int get(const char* ch, int len)
		{
			int pos = ypos(ch,len);
			lock.rdlock();
			int innerseq= this->blocks[pos]->get(ch,len);
			lock.unrdlock();
			return innerseq;
		}
		int add(const char* ch, int len)
		{
			int pos = ypos(ch, len);
			lock.wrlock();
			int innerseq = this->blocks[pos]->insert(ch, len);
			lock.unwrlock();
			return innerseq;
		}
		bool remove(const char* ch,int len)
		{
			int pos = ypos(ch, len);
			lock.wrlock();
			int innerseq = this->blocks[pos]->remove(ch,len);
			lock.unwrlock();
			return innerseq;
		}
	};

}
#endif
