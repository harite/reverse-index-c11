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
	typedef unsigned int uint;
	typedef long long int64;
	const int MAX_NODE_SIZE = 512;
	const int SHARD_SIZE = 1024 * 1024;
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};

	class node
	{
	public:
		//偏移地址
		int offset{ 0 };
		//数据总总长度
		int length{ 0 };
		//主键长度
		short keylen { 0 };
		node() = default;
		~node() = default;
		void set(int offset, short keylen, int length)
		{
			this->offset = offset;
			this->keylen = keylen;
			this->length = length;
		}

		inline int compare(char* chs, const char* key, short klen)
		{
			if (this->keylen == klen)
			{
				for (int i = 0; i < klen; i++)
				{
					if (chs[this->offset + i] == key[i])
					{
						continue;
					}
					else if (chs[this->offset + i] > key[i])
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
				return this->keylen - klen;
			}

		}
	};

	class page
	{
	public:
		char* mems;
		int delSize;
		int menSize;
		int menLength;
		node* nodes;

		int nodeSize;
		int nodeLength;
		char* shardCopyData;
		void ensurenodecapacity(int length)
		{
			if (length > this->nodeLength)
			{
				this->nodeLength = length + 64;
				node* temp = new node[this->nodeLength];
				memmove(temp, nodes, sizeof(node) * this->nodeSize);
				delete[] this->nodes;
				this->nodes = temp;
			}
		}
		void ensuredatacapacity(int length)
		{
			if (length > this->menLength)
			{
				//需要增加的容量
				int needSize = length - this->menSize;
				char* temp = nullptr;
				//如果被删除的数据量三十条 或被删除的数量大于总量的十分之一
				bool clearDelData = this->delSize > needSize * 30 || (this->delSize > needSize && this->delSize * 10 > length);
				if (clearDelData && (this->menSize - this->delSize < SHARD_SIZE))
				{
					temp = this->shardCopyData;
				}
				else 
				{
					this->menLength = length + (this->menLength / (nodeSize + 1)) * 128 + 128;
					temp = new char[this->menLength];
				}
				if (this->delSize > 0)
				{
					int offset = 0;
					for (int i = 0; i < this->nodeSize; i++)
					{
						memmove(temp + offset, mems + nodes[i].offset, sizeof(char) * nodes[i].length);
						nodes[i].set(offset, nodes[i].keylen, nodes[i].length);
						offset += nodes[i].length;
					}
					this->delSize = 0;
					this->menSize = offset;
					//如果是使用了临时拷贝区的话，整理完数据后需要拷贝回原数据块
					if (clearDelData && (this->menSize - this->delSize < SHARD_SIZE))
					{
						memmove(mems, temp, sizeof(char) * this->menSize);
					}
					else 
					{
						delete[] this->mems;
						this->mems = temp;
					}
				}
				else
				{
					memmove(temp, mems, sizeof(char) * this->menSize);
					delete[] this->mems;
					this->mems = temp;
				}
				
			}
		}
	public:
		page(int nlength,int mlength, char* shardCopyData)
		{
			this->nodeSize = 0;
			this->nodeLength = nlength;
			this->nodes = new node[this->nodeLength];

			this->delSize = 0;
			this->menSize = 0;
			this->menLength = mlength;
			this->mems = new char[this->menLength];
			this->shardCopyData = shardCopyData;
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

		void set(node& node, const char* key, short klen, const char* value, int vlen)
		{
			node.set(this->menSize, klen, klen + vlen);
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
					int length = klen + vlen;
					if (nodes[index].length >= length)
					{
						this->delSize += nodes[index].length - length;
						this->nodes[index].length = length;
						memmove(mems + nodes[index].offset + nodes[index].keylen, value, vlen);
						
					}
					else
					{
						this->delSize += this->nodes[index].length;
						this->set(nodes[index], key, klen, value, vlen);
					}
					return false;
				}
				else
				{
					int moveNum = this->nodeSize - index;
					if (moveNum > 0)
					{
						memmove(this->nodes + index + 1, this->nodes + index, sizeof(node) * moveNum);
					}
					this->set(this->nodes[index], key, klen, value, vlen);
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
					this->delSize -= this->nodes[index].length;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(node) * moveNum);
					}
					this->nodeSize--;
					if (this->nodeLength > (this->nodeSize + 3) * 3)
					{
						this->nodeLength = (this->nodeSize + 3) * 2;
						node* temp = new node[this->nodeLength];
						memmove(temp, nodes, sizeof(node) * this->nodeSize);
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
							memmove(temp + offset, mems + nodes[i].offset, sizeof(char) *  nodes[i].length);
							nodes[i].set(offset, nodes[i].keylen, nodes[i].length);
							offset += nodes[i].length;
						}
						this->delSize = 0;
						this->menSize = offset;
					}
					return true;
				}
			}
		}

		const char* get(const char* key, short klen, int& vlen)
		{
			if (this->nodeSize == 0)
			{
				vlen = 0;
				return nullptr;
			}
			int index = indexof(key, klen, type_index);
			if (index >= 0)
			{
				vlen = nodes[index].length- nodes[index].keylen;
				return  mems + nodes[index].offset + nodes[index].keylen;
			}
			else
			{
				vlen = 0;
				return nullptr;
			}
		}

		bool get(const char* key, short klen, string& word)
		{
			int vlen = 0;
			const char* temp = get(key,klen,vlen);
			if (temp != null) {
				word.append(temp, vlen);
				return true;
			}else{
				return false;
			}
		}

		int rangecontains(const char* key,int klen)
		{
			if (nodes[this->nodeSize-1].compare(mems, key, klen) <0)
			{
				return -1;
			}
			else if (nodes[0].compare(mems, key, klen)> 0 )
			{
				return 1;
			}
			return 0;
		}

		int countMemSize(int from, int to)
		{
			int sum = 0;
			for (int i = from; i < to; i++)
			{
				sum += this->nodes[i].length;
			}
			return sum;
		}
		void copyTo(int from, int to, page* pg)
		{
			int offset = 0;
			for (int i = from; i < to; i++)
			{
				pg->nodes[i - from].set(offset,this->nodes[i].keylen,this->nodes[i].length);
				memmove(pg->mems + offset, this->mems+ this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
				offset += this->nodes[i].length;
			}
			//设置当前内存的偏移量
			pg->menSize = offset;
		}

		page** splitToTwo(bool tail)
		{
			page** pages = new page*[2];
			int mid = (this->nodeSize * (tail ? 9 : 5)) / 10;
			int mlength0 = this->countMemSize(0,mid);
			int avg0 = mlength0 / mid;
			pages[0] = new page(mid + 10, mlength0 +10 * avg0, this->shardCopyData);
			pages[0]->nodeSize = mid;
			this->copyTo(0,mid,pages[0]);

			int size = this->nodeSize - mid;
			int mlength1 = this->countMemSize(mid, this->nodeSize);
			int avg1 = mlength1 / size;
			pages[1] = new page(size + 16, mlength1 +16 * avg1, this->shardCopyData);
			pages[1]->nodeSize = size;
			this->copyTo(mid, this->nodeSize, pages[1]);
			return pages;
		}
	};

	class block
	{
	private:
		int size;
		page** pages;
		char* shardCopyData;
		inline int indexOf(page** pages, int size, const char* key, int len, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = pages[mid]->rangecontains(key, len);
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
		block(char* shardCopyData)
		{
			this->size = 1;
			this->pages = new page*[1];
			this->shardCopyData = shardCopyData;
			this->pages[0] = new page(16,16*256, shardCopyData);
		
		}
		~block()
		{
			for (int i = 0; i < this->size; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
		}

		inline int insertAndSplit(int index, const char* key, short klen,const char* value,int vlen)
		{

			int reuslt = this->pages[index]->insert(key, klen,value,vlen);
			if (this->pages[index]->nodeSize > MAX_NODE_SIZE)
			{
				page** temp = this->pages[index]->splitToTwo(index == this->size - 1);
				page** newpages = new page*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(page*)*index);
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
			page* temp0 = this->pages[index0];
			page* temp1 = this->pages[index1];
			int size0 = temp0->nodeSize;
			int size1 = temp1->nodeSize;
			int mlength0 = temp0->countMemSize(0,size0);
			int mlength1 = temp1->countMemSize(0, size1);
			page* newpage = new page( size0 + size1 + MAX_NODE_SIZE/8, mlength0+ mlength1+ mlength1/10,this->shardCopyData);
		
			int offset = 0;
			for (int i = 0; i < size0; i++)
			{
				newpage->nodes[i].set(offset, temp0->nodes[i].keylen, temp0->nodes[i].length);
				memmove(newpage->mems+ offset, temp0->mems + temp0->nodes[i].offset, sizeof(char) * temp0->nodes[i].length);
				offset += temp0->nodes[i].length;
			}

			for (int i = 0; i < size1; i++)
			{
				newpage->nodes[i+size0].set(offset, temp1->nodes[i].keylen, temp1->nodes[i].length);
				memmove(newpage->mems+ offset, temp1->mems + temp1->nodes[i].offset, sizeof(char) * temp1->nodes[i].length);
				offset += temp1->nodes[i].length;
			}

			//设置新也的数据量
			newpage->nodeSize = size0 + size1;
			newpage->menSize = offset;
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
				memmove(this->pages + index1, this->pages + index1 + 1, sizeof(page*)*moveNum);
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
			else if (this->pages[index]->nodeSize < (MAX_NODE_SIZE / 32))
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

		int insert(const char* key, short klen,const char* value,int vlen)
		{
			if (this->size == 1 || this->pages[0]->rangecontains(key, klen) >= 0)
			{
				return this->insertAndSplit(0, key, klen,value,vlen);
			}
			else if (this->pages[this->size - 1]->rangecontains(key, klen) <= 0)
			{
				return this->insertAndSplit(this->size - 1, key, klen,value,vlen);
			}
			else
			{
				int pageno = indexOf(pages, this->size, key, klen, type_ceil);
				return this->insertAndSplit(pageno, key, klen,value,vlen);
			}
		}

		bool remove(const char* key, int len)
		{
			if (this->pages[this->size - 1]->rangecontains(key, len) == 0)
			{
				bool deled = this->pages[this->size - 1]->remove(key, len);
				this->combine(this->size - 1);
				return deled;
			}
			else
			{
				int pageno = indexOf(this->pages, this->size, key, len, type_index);
				if (pageno >= 0)
				{
					bool deled = this->pages[pageno]->remove(key, len);
					this->combine(pageno);
					return deled;
				}
				return false;
			}
		}
		const char* get(const char* key, int klen,int& vlen)
		{
			int pageno = indexOf(this->pages, this->size, key, klen, type_index);
			if (pageno >= 0) {
				return this->pages[pageno]->get(key, klen,vlen);
			}
			else {
				return nullptr;
			}
		}

	};
	class dics2s
	{
	private:

		uint partition;
		block** blocks;
		char* shardCopyData;
		std::atomic<int> size { 0 };
		qstardb::rwsyslock lock;
		inline uint _hash(const char* str, short len)
		{
			uint hash = 0;
			while (len-- > 0)
			{
				hash = hash * 31 + (*str++);
			}
			return hash;
		}

		inline unsigned int index(const char* key, short len)
		{
			int value= _hash(key, len) % partition;
			if (value < 0) {
				return -value;
			}
			else {
				return value;
			}
		}
	public:
		dics2s(uint partition)
		{
			this->size = 0;
			this->partition = partition;
			this->blocks = new block*[this->partition];
			this->shardCopyData = new char[SHARD_SIZE];
			for (uint i = 0; i < partition; i++)
			{
				this->blocks[i] = new block(shardCopyData);
			}
		}
		~dics2s()
		{
			for (uint i = 0; i < partition; i++)
			{
				delete this->blocks[i];
			}
			delete[] this->blocks;
			delete[] this->shardCopyData;
		}

		int remove(const char* key, short klen)
		{
			uint pos = index(key, klen);
			lock.wrlock();
			if (blocks[pos]->remove(key, klen))
			{
				this->size--;
			}
			lock.unwrlock();
			return this->size;
		}

		int insert(const char* key, short klen, const char* value, int vlen)
		{
			uint pos = index(key, klen);
			lock.wrlock();
			if (blocks[pos]->insert(key, klen, value, vlen))
			{
				this->size++;
			}
			lock.unwrlock();
			return this->size;
		}

		bool get(const char* key, short klen, qstardb::charwriter* writer)
		{
			int vlen = -1;
			lock.rdlock();
			const char* temp=blocks[index(key, klen)]->get(key, klen, vlen);
			if (temp != nullptr)
			{
				writer->writeInt(vlen);
				writer->write(temp,vlen);
				lock.unrdlock();
				return true;
			}
			lock.unrdlock();
			return false;
		}

		bool get(const char* key, short klen, string& word)
		{
			int vlen;
			uint pos = index(key, klen);
			lock.rdlock();
			const char* ch = blocks[pos]->get(key, klen, vlen);
			if (ch != nullptr)
			{
				word.append(ch,vlen);
				lock.unrdlock();
				return true;
			}
			else {
				lock.unrdlock();
				return false;
			}
		}

		short readshort(const char* ch, int offset)
		{
			return (((short) ch[offset]) << 8) | (short) (ch[offset + 1] & 0xff);
		}

		int docszie()
		{
			return this->size;
		}
	};
}
#endif
