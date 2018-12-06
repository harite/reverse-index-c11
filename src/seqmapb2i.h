#pragma once
#ifndef _SEQMAP_B2I_H
#define _SEQMAP_B2I_H
#include "sequence.h"
#include "seqmapi2b.h"
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
		int indexof(uint* keys, int size, const char* key, int len,  type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int tempLength;
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

		int inset(const char* ch,int len)
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
				return -1;
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
					return -1;
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
	};
	class b2imap
	{
	private:
		int partition;
		seqblock* _block;
		b2ipage** pages;
		qstardb::sequence* seq;
		qstardb::rwsyslock rwlock;
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
			this->pages = new b2ipage*[part];
			for (int i = 0; i < part; i++)
			{
				this->pages[i] = new b2ipage(this->seq,1024, this->_block);
			}
		}
		~b2imap()
		{
			for (int i = 0; i < this->partition; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
			delete this->seq;
			delete this->_block;
		}
		int curSeq()
		{
			return this->seq->get();
		}
		bool get(int index,string& str)
		{
			int length;
			rwlock.rdlock();
			const char* temp=this->_block->find(index, length);
			if (temp != nullptr)
			{
				str.append(temp,length);
				rwlock.unrdlock();
				return true;
			}
			else
			{
				rwlock.unrdlock();
				return false;
			}
		}
		int get(const char* ch, int len)
		{
			int pos = ypos(ch,len);
			rwlock.rdlock();
			int result= this->pages[pos]->get(ch,len);
			rwlock.unrdlock();
			return result;
		}
		int add(const char* ch, int len)
		{
			int pos = ypos(ch, len);
			rwlock.wrlock();
			int result= this->pages[pos]->inset(ch, len);
			rwlock.unwrlock();
			return result;
		}
		bool remove(const char* ch,int len)
		{
			int pos = ypos(ch, len);
			rwlock.wrlock();
			int result = this->pages[pos]->remove(ch,len);
			rwlock.unwrlock();
			return result;
		}
	};

}
#endif
