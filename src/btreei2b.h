#pragma once
#ifndef BTREEI2B_H_
#define BTREEI2B_H_
#include <string.h>
namespace btree
{
	typedef long long int64;

	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};

	class node
	{
	public:
		int64 key{ 0 };
		int offset{ 0 };
		int length{ 0 };
		void set(int64 key, int offset, int length)
		{
			this->key = key;
			this->offset = offset;
			this->length = length;
		}
	};
	class page
	{
	private:
		//元素个数
		int nodesize{ 0 };
		//元素空间总长度
		int nodelength{ 1024 };
		//数据已经被删除
		int deldatasize{ 0 };
		//已经使用的数据
		int useddatasize{ 0 };
		//数据空间总长度
		int datalength{ 1024 * 128 };
		//node节点
		node* nodes;
		//数据存放空间
		char* datas;
		//数据交换空间，临时数据空间
		char* shardData;
		//临时数据空间长度
		int shardLength{ 0 };
	public:
		page(int nodelength, int datalength)
		{
			this->nodelength = nodelength;
			this->datalength = datalength;
			this->nodes = new node[this->nodelength];
			this->datas = new char[this->datalength];
			
		}
		~page()
		{
			delete[] this->nodes;
		}
		inline int compare(int64 key1, int64 key2)
		{
			return key1 == key2 ? 0 : key1 > key2 ? 1 : -1;
		}

		void ensureCapacity(int nodeCapacity, int dataCapacity)
		{
			//节点数量不足了

			int oldCapacity = this->nodelength;
			int minCapacity = this->nodesize + nodeCapacity;
			if (minCapacity > oldCapacity)
			{
				int extcapacity = oldCapacity >> 1;
				this->nodelength = oldCapacity + (extcapacity > 256 ? 256 : extcapacity) + 32;
				if (this->nodelength < minCapacity)
				{
					this->nodelength = minCapacity;
				}
				node* temp = new node[this->nodelength];
				memcpy(temp, nodes, sizeof(node) * this->nodesize);
				delete[] this->nodes;
				this->nodes = temp;
			}
			//内容不能直接分配了
			int oldLength = this->datalength;
			int minDataCapacity = this->useddatasize + dataCapacity;
			if (minDataCapacity > oldLength)
			{
				//如果被删除的数据空间大于需要分配的空间
				if (this->deldatasize >= dataCapacity)
				{
					//使用共享空间做为临时存储
					bool useShardData = this->datalength < this->shardLength;
					//临时存储数据空间的地址
					char* tempData = useShardData ? shardData : new char[this->datalength];
					memcpy(tempData, datas, sizeof(char) * datalength);
					this->deldatasize = 0;
					this->useddatasize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < nodesize; i++)
					{
						memcpy(datas + useddatasize, tempData + nodes[i].offset, sizeof(char) * this->nodes[i].length);
						useddatasize += this->nodes[i].length;
					}
					//如果不是使用的共享空间，则释放临时内存
					if (!useShardData)
					{
						delete[] tempData;
					}
				}
				else {//直接申请新的存储空间，进行数据扩容
					int usedCapacity = this->useddatasize - this->deldatasize;
					int extcapacity = usedCapacity >> 1;
					this->nodelength = usedCapacity + (extcapacity > 64 * 1024 ? 64 * 1024 : extcapacity);
					if (this->nodelength < usedCapacity + dataCapacity)
					{
						this->nodelength = usedCapacity + dataCapacity;
					}
					char* tempData = new char[this->nodelength];
					//重新申请了内存就重新清理
					this->deldatasize = 0;
					this->useddatasize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < nodesize; i++)
					{
						memcpy(tempData + useddatasize, datas + nodes[i].offset, sizeof(char) * this->nodes[i].length);
						useddatasize += this->nodes[i].length;
					}
					delete[] this->datas;
					this->datas = tempData;
				}
			}
		}

		inline int indexof(node* nodes, int64 key, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = compare(nodes[mid].key, key);
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

		bool insert(int64 key, const char* ch, int length)
		{
			if (this->nodesize == 0 || this->compare(key, nodes[this->nodesize - 1].key) > 0)
			{
				//确认空间足够
				ensureCapacity(1, length);
				this->nodes[this->nodesize].set(key, this->useddatasize, length);
				memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
				this->nodesize++;
				return true;
			}
			else if (this->compare(key, nodes[0].key) < 0)
			{
				ensureCapacity(1, length);
				memmove(this->nodes + 1, this->nodes, sizeof(node) * this->nodesize);
				this->nodes[0].set(key, this->useddatasize, length);
				memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
				this->nodesize++;
				return true;
			}
			else
			{
				int pos = indexof(this->nodes, key, this->nodesize, type_insert);
				if (pos > 0) {
					int numMoved = this->nodesize - pos;
					if (numMoved > 0)
					{
						memmove(this->nodes + pos + 1, this->nodes + pos, sizeof(node) * numMoved);
					}
					this->nodes[pos].set(key, this->useddatasize, length);
					memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
					this->nodesize++;
					return true;
				}
				else {//key foud，数据已经存在，则覆盖原数据
					int index = -pos - 1;
					if (this->nodes[index].length >= length)
					{
						this->deldatasize += this->nodes[index].length- length;
						//数据长度变短了
						this->nodes[index].set(key, this->nodes[index].offset, length);
						memmove(this->datas + this->nodes[index].offset, ch, sizeof(char) * length);
					}
					else
					{
						//空间已经不足，重新分配
						ensureCapacity(1, length);
						this->deldatasize += this->nodes[index].length;
						this->nodes[index].set(key, this->useddatasize, length);
						memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
					}
				}
				
			}

		}
		inline bool contains(int64 key)
		{
			return indexof(nodes, key, this->nodesize, type_index) >= 0;
		}

		bool remove(int64 key)
		{
			if (this->nodesize == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(nodes, key, this->nodesize, type_index);
				if (index >= 0)
				{
					int moveNum = this->nodesize - index - 1;
					if (moveNum > 0)
					{
						this->deldatasize += this->nodes[index].length;
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(node) * moveNum);
					}
					else
					{
						this->useddatasize -= this->nodes[index].length;
					}
					this->nodesize--;
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		int countDataSize(int from, int to)
		{
			int sum = 0;
			for (size_t i = from; i < to && i < this->nodesize; i++)
			{
				sum += this->nodes[i].length;
			}
			return sum;
		}

		int copyTo( int from, int to, page* page) {
			for (size_t i = from, j = 0; i < to && i < this->nodesize; i++)
			{
				this->nodes[j++].set(this->nodes[i].key, page->useddatasize, this->nodes[i].length);
				memmove(page->datas + page->useddatasize, this->datas + this->nodes[i].offset, sizeof(char) *  this->nodes[i].length);
				page->useddatasize += this->nodes[i].length;
			}
		}

		page** splitToTwo(bool tail)
		{
			page** pages = new page*[2];
			int mid = (this->nodesize * (tail ? 7 : 5)) / 10;
			int datasize1 = countDataSize(0, mid) * 11 / 10;
			pages[0] = new page(mid*11/10, datasize1);
			pages[0]->nodesize = mid;
			this->copyTo(0,mid,pages[0]);

			int datasize2 = countDataSize(mid, this->nodesize) * 12 / 10;
			pages[1] = new page((this->nodesize- mid)*12 /10, datasize2);
			pages[1]->nodesize = this->nodesize - mid;
			this->copyTo(mid, this->nodesize, pages[1]);
			return pages;
		}

		/*判定页的范围是否包含 key*/
		int rangecontains(int64 key)
		{
			if (this->compare(this->nodes[this->nodesize - 1].key, key) < 0)
			{
				return -1;
			}
			else if (this->compare(this->nodes[0].key, key) > 0)
			{
				return 1;
			}
			return 0;
		}
	};

	class block
	{
		int size;
		page** pages;
	public:
		block()
		{
			this->size = 1;
			this->pages = new page*[1];
			this->pages[0] = new page();
		}
		~block()
		{
			for (int i = 0; i < size; i++)
			{
				delete pages[i];
			}
			delete[] pages;
		}
	};


}
#endif
