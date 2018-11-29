#pragma once
#ifndef BTREEI2B_H_
#define BTREEI2B_H_
#include <string.h>
namespace btree
{
	typedef long long int64;
	//临时共享空间，用于数据垃圾清理临时存储数据
	static const int  SHARD_SIZE  = 1024 * 1024 * 4;
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
		//元素空间总长度
		int nodelength{ 1024 };
		//数据空间总长度
		int datalength{ 1024 * 128 };
		//数据交换空间，临时数据空间
		char* shardData;
		//临时数据空间长度
		int shardLength{ 0 };
	public:
		//已经使用的数据
		int useddatasize{ 0 };
		//元素个数
		int nodesize{ 0 };
		//数据已经被删除
		int deldatasize{ 0 };
		//node节点，记录数据的主键，value存储的偏移地址以及长度大小
		node* nodes;
		//数据存放空间
		char* datas;
		int dataSize()
		{
			return this->useddatasize - this->deldatasize;
		}
	public:
		page(int nodelength, int datalength,char* shardData,int shardLength)
		{
			this->nodelength = nodelength;
			this->datalength = datalength;
			this->shardData = shardData;
			this->shardLength = shardLength;
			this->nodes = new node[this->nodelength];
			this->datas = new char[this->datalength];
		}
		~page()
		{
			delete[] this->nodes;
			delete[] this->datas;
		}
		inline int size()
		{
			return this->nodesize;
		}
		inline int compare(int64 key1, int64 key2)
		{
			return key1 == key2 ? 0 : (key1 > key2 ? 1 : -1);
		}

		void ensureCapacity(int nodeCapacity, int dataCapacity)
		{
			int oldCapacity = this->nodelength;
			int minCapacity = this->nodesize + nodeCapacity;
			//如果节点数量不足，则需要扩容
			if (minCapacity > oldCapacity)
			{
				int extcapacity = oldCapacity >> 1;
				this->nodelength = oldCapacity + (extcapacity > 256 ? 256 : extcapacity) + 32;
				//扩容量最大为256+32条数据的空间
				if (this->nodelength < minCapacity)
				{
					this->nodelength = minCapacity;
				}
				node* temp = new node[this->nodelength];
				memmove(temp, nodes, sizeof(node) * this->nodesize);
				delete[] this->nodes;
				this->nodes = temp;
			}
	
			int oldLength = this->datalength;
			int minDataCapacity = this->useddatasize + dataCapacity;
			//数据存储空间不足，需要创新分配容量
			if (minDataCapacity > oldLength)
			{
				//如果被已删除的占位数据空间大于需要分配的空间，则清理空间
				if (this->deldatasize >= dataCapacity)
				{
					//使用共享空间做为临时存储
					bool useShardData = this->useddatasize < this->shardLength;
					//临时存储数据空间的地址
					char* tempData = useShardData ? shardData : new char[this->useddatasize];
					memmove(tempData, this->datas, sizeof(char) * this->useddatasize);
					this->deldatasize = 0;
					this->useddatasize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < this->nodesize; i++)
					{
						memmove(datas + this->useddatasize, tempData + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
						this->nodes[i].set(this->nodes[i].key, this->useddatasize, this->nodes[i].length);
						this->useddatasize += this->nodes[i].length;
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
					this->datalength = usedCapacity + (extcapacity > 64 * 1024 ? 64 * 1024 : extcapacity);
					if (this->datalength < usedCapacity + dataCapacity)
					{
						this->datalength = usedCapacity + dataCapacity;
					}
					char* tempData = new char[this->datalength];
					//重新申请了内存就重新清理
					this->deldatasize = 0;
					this->useddatasize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < this->nodesize; i++)
					{
						memcpy(tempData + this->useddatasize, datas + nodes[i].offset, sizeof(char) * this->nodes[i].length);
						this->nodes[i].set(this->nodes[i].key, this->useddatasize, this->nodes[i].length);
						this->useddatasize += this->nodes[i].length;
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
				this->useddatasize += length;
				this->nodesize++;
				return true;
			}
			else if (this->compare(key, nodes[0].key) < 0)
			{
				ensureCapacity(1, length);
				memmove(this->nodes + 1, this->nodes, sizeof(node) * this->nodesize);
				this->nodes[0].set(key, this->useddatasize, length);
				memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
				this->useddatasize += length;
				this->nodesize++;
				return true;
			}
			else
			{
				int pos = indexof(this->nodes, key, this->nodesize, type_insert);
				if (pos >= 0) {
					ensureCapacity(1, length);
					int numMoved = this->nodesize - pos;
					if (numMoved > 0)
					{
						memmove(this->nodes + pos + 1, this->nodes + pos, sizeof(node) * numMoved);
					}
					this->nodes[pos].set(key, this->useddatasize, length);
					memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
					this->useddatasize += length;
					this->nodesize++;
					return true;
				}
				else {//key foud，数据已经存在，则覆盖原数据
					int index = -pos - 1;
					if (this->nodes[index].length >= length)
					{
						if(this->nodes[index].length > length)
						cout << "长度变短了？" << endl;
						this->deldatasize += this->nodes[index].length- length;
						//数据长度变短了
						this->nodes[index].set(key, this->nodes[index].offset, length);
						memmove(this->datas + this->nodes[index].offset, ch, sizeof(char) * length);
					}
					else
					{
						//空间已经不足，重新分配
						ensureCapacity(0, length);
						this->deldatasize += this->nodes[index].length;
						this->nodes[index].set(key, this->useddatasize, length);
						memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
						this->useddatasize += length;
					}
					return false;
				}
				
			}

		}
		const char* find(int64 key,int& length)
		{
			int index = indexof(nodes,key,this->nodesize,type_index);
			if (index >= 0) {
				length = this->nodes[index].length;
				return this->datas + this->nodes[index].offset;
			}
			else {
				return nullptr;
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
				//如果找到数据，则进行操作
				int index = indexof(nodes, key, this->nodesize, type_index);
				if (index >= 0)
				{
					//标记被删除的数据，讲数据空间纳入回收容量
					this->deldatasize += this->nodes[index].length;
					int moveNum = this->nodesize - index - 1;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(node) * moveNum);
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

		void copyTo( int from, int to, page* page) {
			for (size_t i = from, j = 0; i < to && i < this->nodesize; i++)
			{
				page->nodes[j++].set(this->nodes[i].key, page->useddatasize, this->nodes[i].length);
				memmove(page->datas + page->useddatasize, this->datas + this->nodes[i].offset, sizeof(char) *  this->nodes[i].length);
				page->useddatasize += this->nodes[i].length;
			}
		}

		page** splitToTwo(bool tail)
		{
			page** pages = new page*[2];
			int mid = (this->nodesize * (tail ? 7 : 5)) / 10;
			int datasize1 = countDataSize(0, mid) * 11 / 10;
			pages[0] = new page(mid*11/10, datasize1, this->shardData, this->shardLength);
			pages[0]->nodesize = mid;
			this->copyTo(0,mid,pages[0]);

			int datasize2 = countDataSize(mid, this->nodesize) * 12 / 10;
			pages[1] = new page((this->nodesize- mid)*12 /10, datasize2,this->shardData,this->shardLength);
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
		//每个页最大多少条数据
		int max_nodenum_per_page;
		//每条数据的平均大小
		int avg_node_data_size;

		page** pages;
		char* shardData;
	private:
		inline int indexof(page** pages, int64 key, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = pages[mid]->rangecontains(key);
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
		block(int max_nodenum_per_page,int avg_node_data_size)
		{
			this->size = 1;
			this->pages = new page*[1];
			this->shardData = new char[SHARD_SIZE];
			this->max_nodenum_per_page = max_nodenum_per_page;
			this->avg_node_data_size = avg_node_data_size;
			this->pages[0] = new page(max_nodenum_per_page/4, max_nodenum_per_page / 4 * avg_node_data_size, shardData, SHARD_SIZE);
		}
		~block()
		{
			for (int i = 0; i < size; i++)
			{
				delete pages[i];
			}
			delete[] pages;
			delete[] shardData;
		}

		void insertAndSplit(int index, int64 key,const char* data, int length)
		{
		
			this->pages[index]->insert(key,data,length);
			if (this->pages[index]->size()>this->max_nodenum_per_page)
			{
				cout << "insertAndSplit.splitToTwo" << endl;
				page** temp = this->pages[index]->splitToTwo(index==this->size-1);
				page** newpages = new page*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages,this->pages,sizeof(page*)*index);
				}
				int moveNum = this->size - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(page*) * moveNum);
				}
				newpages[index] = temp[0];
				newpages[index+1] = temp[1];
				delete pages[index];
				delete[] temp;
				delete[] this->pages;
				this->pages = newpages;
				this->size++;
			}
		}

		void insert(int64 key,const char* data,int length)
		{
			if (this->size == 1 || this->pages[0]->rangecontains(key) >= 0)
			{
				this->insertAndSplit(0, key, data, length);
			}
			else if (this->pages[this->size - 1]->rangecontains(key) <= 0)
			{
				this->insertAndSplit(this->size - 1, key, data, length);
			}
			else
			{
				int pageno = indexof( this->pages, key,  this->size, type_ceil);
				this->insertAndSplit(pageno, key, data, length);
			}
		}

		bool combine(int index0, int index1)
		{
			page* temp0 = this->pages[index0];
			page* temp1 = this->pages[index1];
			int size0 = temp0->size();
			int size1 = temp1->size();
			int datasize0 = temp0->dataSize();
			int datasize1 = temp1->dataSize();
			page* newpage = new page(size0 + size1, datasize0 + datasize1, shardData, SHARD_SIZE);
		
			if (temp0->deldatasize > 0)
			{
				for (size_t i = 0; i < temp0->nodesize; i++)
				{
					newpage->nodes[i].set(temp0->nodes[i].key, newpage->useddatasize, temp0->nodes[i].length);
					memmove(newpage->datas+newpage->useddatasize, temp0->datas+temp0->nodes[i].offset,sizeof(char)*temp0->nodes[i].length);
					newpage->useddatasize += temp0->nodes[i].length;
				}
			}
			else
			{
				memmove(newpage->datas + newpage->useddatasize, temp0->datas,sizeof(char)*temp0->useddatasize);
				newpage->useddatasize += temp0->useddatasize;
			}
			for (size_t i = 0; i < temp1->nodesize; i++)
			{
				newpage->nodes[i+ temp0->nodesize].set(temp1->nodes[i].key, newpage->useddatasize, temp1->nodes[i].length);
				memmove(newpage->datas + newpage->useddatasize, temp1->datas+ temp1->nodes[i].offset, sizeof(char)*temp1->nodes[i].length);
				newpage->useddatasize += temp1->nodes[i].length;
			}
			//设置新也的数据量
			newpage->nodesize = temp0->nodesize + temp1->nodesize;
			//释放原数据页1
			delete temp0;
			//释放原数据页2
			delete temp1;
			//将新页数据放到 index0的位置
			this->pages[index0] = newpage;
			//将空白页删除
			int moveNum = this->size - index1-1;
			if (moveNum > 0)
			{
				memmove(this->pages + index1 , this->pages+index1+1 , sizeof(page*)*moveNum);
			}
			this->size--;
			return true;
		}

		bool combine(int index)
		{
			if (this->size == 1)
			{
				return false;
			}
			//如果数据页内的数据小于规定最大数据量的32分之一，则合并数据页
			else if (this->pages[index]->size() < (this->max_nodenum_per_page / 64))
			{
				//如果是第一页，则将第二页的合并到第一页
				if (index == 0)
				{
					combine(0,1);
				}
				else//其他情况则将数据将与前一页合并
				{
					combine(index-1, index );
				}
			}
			else 
			{
				return false;
			}
		}



		void remove(int64 key)
		{
			if (this->pages[this->size - 1]->rangecontains(key) == 0)
			{
				this->pages[this->size - 1]->remove(key);
				this->combine(this->size - 1);	
			}
			else
			{
				int pageno = indexof( this->pages, key,  this->size, type_index);
				if (pageno >= 0)
				{
					this->pages[pageno]->remove(key);
					this->combine(pageno);
				}
			}
		}

		const char* find(int64 key,int& length)
		{
			int pageno = indexof(this->pages, key, this->size, type_index);
			if (pageno >= 0)
			{
				return this->pages[pageno]->find(key,length);
			}
			else 
			{
				return nullptr;
			}
		}
	};
}
#endif
