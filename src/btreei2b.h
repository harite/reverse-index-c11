#pragma once
#ifndef BTREEI2B_H_
#define BTREEI2B_H_
#include <string.h>
namespace btree
{
	typedef long long int64;
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

		//Ԫ�ؿռ��ܳ���
		int nodelength{ 1024 };
		//���ݿռ��ܳ���
		int datalength{ 1024 * 128 };
		//node�ڵ�
	
		//���ݽ����ռ䣬��ʱ���ݿռ�
		char* shardData;
		//��ʱ���ݿռ䳤��
		int shardLength{ 0 };
	public:
		//�Ѿ�ʹ�õ�����
		int useddatasize{ 0 };
		//Ԫ�ظ���
		int nodesize{ 0 };

		//�����Ѿ���ɾ��
		int deldatasize{ 0 };

		//Ԫ�ظ���
		int nodesize{ 0 };

		node* nodes;
		//���ݴ�ſռ�
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
		}
		inline int size()
		{
			return this->nodesize;
		}
		inline int compare(int64 key1, int64 key2)
		{
			return key1 == key2 ? 0 : key1 > key2 ? 1 : -1;
		}

		void ensureCapacity(int nodeCapacity, int dataCapacity)
		{
			//�ڵ�����������
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
			//���ݲ���ֱ�ӷ�����
			int oldLength = this->datalength;
			int minDataCapacity = this->useddatasize + dataCapacity;
			if (minDataCapacity > oldLength)
			{
				//�����ɾ�������ݿռ������Ҫ����Ŀռ�
				if (this->deldatasize >= dataCapacity)
				{
					//ʹ�ù���ռ���Ϊ��ʱ�洢
					bool useShardData = this->datalength < this->shardLength;
					//��ʱ�洢���ݿռ�ĵ�ַ
					char* tempData = useShardData ? shardData : new char[this->datalength];
					memcpy(tempData, datas, sizeof(char) * datalength);
					this->deldatasize = 0;
					this->useddatasize = 0;
					//�����ݿ�����ԭ���ĵ�ַ
					for (int i = 0; i < nodesize; i++)
					{
						memcpy(datas + useddatasize, tempData + nodes[i].offset, sizeof(char) * this->nodes[i].length);
						useddatasize += this->nodes[i].length;
					}
					//�������ʹ�õĹ���ռ䣬���ͷ���ʱ�ڴ�
					if (!useShardData)
					{
						delete[] tempData;
					}
				}
				else {//ֱ�������µĴ洢�ռ䣬������������
					int usedCapacity = this->useddatasize - this->deldatasize;
					int extcapacity = usedCapacity >> 1;
					this->nodelength = usedCapacity + (extcapacity > 64 * 1024 ? 64 * 1024 : extcapacity);
					if (this->nodelength < usedCapacity + dataCapacity)
					{
						this->nodelength = usedCapacity + dataCapacity;
					}
					char* tempData = new char[this->nodelength];
					//�����������ڴ����������
					this->deldatasize = 0;
					this->useddatasize = 0;
					//�����ݿ�����ԭ���ĵ�ַ
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
				//ȷ�Ͽռ��㹻
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
				else {//key foud�������Ѿ����ڣ��򸲸�ԭ����
					int index = -pos - 1;
					if (this->nodes[index].length >= length)
					{
						this->deldatasize += this->nodes[index].length- length;
						//���ݳ��ȱ����
						this->nodes[index].set(key, this->nodes[index].offset, length);
						memmove(this->datas + this->nodes[index].offset, ch, sizeof(char) * length);
					}
					else
					{
						//�ռ��Ѿ����㣬���·���
						ensureCapacity(1, length);
						this->deldatasize += this->nodes[index].length;
						this->nodes[index].set(key, this->useddatasize, length);
						memmove(this->datas + this->useddatasize, ch, sizeof(char) * length);
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
			pages[0] = new page(mid*11/10, datasize1, this->shardData, this->shardLength);
			pages[0]->nodesize = mid;
			this->copyTo(0,mid,pages[0]);

			int datasize2 = countDataSize(mid, this->nodesize) * 12 / 10;
			pages[1] = new page((this->nodesize- mid)*12 /10, datasize2,this->shardData,this->shardLength);
			pages[1]->nodesize = this->nodesize - mid;
			this->copyTo(mid, this->nodesize, pages[1]);
			return pages;
		}

		/*�ж�ҳ�ķ�Χ�Ƿ���� key*/
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
		//ÿ��ҳ������������
		int page_avg_num;
		//ÿ�����ݵ�ƽ����С
		int node_avg_size;
		page** pages;
		int max_page_num;
		char shardData[SHARD_SIZE];
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
		block(int num_per_page,int page_avg_num,int node_avg_size)
		{
			this->size = 1;
			this->pages = new page*[1];
			this->max_page_num = 1024 * 8;
			this->page_avg_num= page_avg_num;
			this->node_avg_size = node_avg_size;
			this->pages[0] = new page(page_avg_num, node_avg_size*node_avg_size, shardData, SHARD_SIZE);
		}
		~block()
		{
			for (int i = 0; i < size; i++)
			{
				delete pages[i];
			}
			delete[] pages;
		}

		void insertAndSplit(int index, int64 key, char* data, int length)
		{
			this->pages[index]->insert(key,data,length);
			if (this->pages[index]->size()>this->max_page_num)
			{
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

		void insert(int64 key,char* data,int length)
		{
			if (this->size == 1 || this->pages[0]->rangecontains(key) >= 0)
			{
				this->insertAndSplit(0, key, data, length);
			}
			if (this->pages[this->size - 1]->rangecontains(key) <= 0)
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
			int size0 = this->pages[index0]->size();
			int size1 = this->pages[index1]->size();
			int datasize0 = this->pages[index0]->dataSize();
			int datasize1 = this->pages[index1]->dataSize();
			page* newpage = new page(size0 + size1, datasize0 + datasize1, shardData, SHARD_SIZE);
			page* temp0 = this->pages[index0];
			if (this->pages[index0]->deldatasize > 0)
			{
				for (size_t i = 0; i < temp0->nodesize; i++)
				{
					newpage->nodes[i].set(temp0->nodes[i].key, newpage->useddatasize, temp0->nodes[i].length);
					memccpy(newpage->datas+newpage->useddatasize, temp0->datas, temp0->nodes[i].offset,sizeof(char)*temp0->nodes[i].length);
					newpage->useddatasize += temp0->nodes[i].length;
				}
			}
			else
			{
				memccpy(newpage->datas + newpage->useddatasize, temp0->datas,0, sizeof(char)*temp0->useddatasize);
			}
			page* temp1 = this->pages[index1];
			for (size_t i = 0; i < temp1->nodesize; i++)
			{
				newpage->nodes[i+ temp0->nodesize].set(temp1->nodes[i].key, newpage->useddatasize, temp1->nodes[i].length);
				memccpy(newpage->datas + newpage->useddatasize, temp1->datas, temp1->nodes[i].offset, sizeof(char)*temp1->nodes[i].length);
				newpage->useddatasize += temp1->nodes[i].length;
			}
			//������Ҳ��������
			newpage->nodesize = temp1->nodesize + temp1->nodesize;
			//�ͷ�ԭ����ҳ1
			delete temp0;
			//�ͷ�ԭ����ҳ2
			delete temp1;
			//����ҳ���ݷŵ� index0��λ��
			this->pages[index0] = newpage;
			//���հ�ҳɾ��
			int moveNum = this->size - index1-1;
			if (moveNum > 0)
			{
				memmove(this->pages + index1 , this->pages+index1+1 , sizeof(page*)*moveNum);
			}
			this->size--;
		}

		bool combine(int index)
		{
			if (this->size == 1)
			{
				return false;
			}
			//�������ҳ�ڵ�����С�ڹ涨�����������32��֮һ����ϲ�����ҳ
			else if (this->pages[index]->size() < (this->max_page_num/32))
			{
				//����ǵ�һҳ���򽫵ڶ�ҳ�ĺϲ�����һҳ
				if (index == 0)
				{
					combine(0,1);
				}
				else//������������ݽ���ǰһҳ�ϲ�
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
