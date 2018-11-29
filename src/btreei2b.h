#pragma once
#ifndef BTREEI2B_H_
#define BTREEI2B_H_
#include <string.h>
namespace btree
{
	typedef long long int64;
	//��ʱ����ռ䣬������������������ʱ�洢����
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
		//node�ڵ㣬��¼���ݵ�������value�洢��ƫ�Ƶ�ַ�Լ����ȴ�С
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
			//����ڵ��������㣬����Ҫ����
			if (minCapacity > oldCapacity)
			{
				int extcapacity = oldCapacity >> 1;
				this->nodelength = oldCapacity + (extcapacity > 256 ? 256 : extcapacity) + 32;
				//���������Ϊ256+32�����ݵĿռ�
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
			//���ݴ洢�ռ䲻�㣬��Ҫ���·�������
			if (minDataCapacity > oldLength)
			{
				//�������ɾ����ռλ���ݿռ������Ҫ����Ŀռ䣬������ռ�
				if (this->deldatasize >= dataCapacity)
				{
					//ʹ�ù���ռ���Ϊ��ʱ�洢
					bool useShardData = this->useddatasize < this->shardLength;
					//��ʱ�洢���ݿռ�ĵ�ַ
					char* tempData = useShardData ? shardData : new char[this->useddatasize];
					memmove(tempData, this->datas, sizeof(char) * this->useddatasize);
					this->deldatasize = 0;
					this->useddatasize = 0;
					//�����ݿ�����ԭ���ĵ�ַ
					for (int i = 0; i < this->nodesize; i++)
					{
						memmove(datas + this->useddatasize, tempData + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
						this->nodes[i].set(this->nodes[i].key, this->useddatasize, this->nodes[i].length);
						this->useddatasize += this->nodes[i].length;
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
					this->datalength = usedCapacity + (extcapacity > 64 * 1024 ? 64 * 1024 : extcapacity);
					if (this->datalength < usedCapacity + dataCapacity)
					{
						this->datalength = usedCapacity + dataCapacity;
					}
					char* tempData = new char[this->datalength];
					//�����������ڴ����������
					this->deldatasize = 0;
					this->useddatasize = 0;
					//�����ݿ�����ԭ���ĵ�ַ
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
				//ȷ�Ͽռ��㹻
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
				else {//key foud�������Ѿ����ڣ��򸲸�ԭ����
					int index = -pos - 1;
					if (this->nodes[index].length >= length)
					{
						if(this->nodes[index].length > length)
						cout << "���ȱ���ˣ�" << endl;
						this->deldatasize += this->nodes[index].length- length;
						//���ݳ��ȱ����
						this->nodes[index].set(key, this->nodes[index].offset, length);
						memmove(this->datas + this->nodes[index].offset, ch, sizeof(char) * length);
					}
					else
					{
						//�ռ��Ѿ����㣬���·���
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
				//����ҵ����ݣ�����в���
				int index = indexof(nodes, key, this->nodesize, type_index);
				if (index >= 0)
				{
					//��Ǳ�ɾ�������ݣ������ݿռ������������
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
		int max_nodenum_per_page;
		//ÿ�����ݵ�ƽ����С
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
			//������Ҳ��������
			newpage->nodesize = temp0->nodesize + temp1->nodesize;
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
			return true;
		}

		bool combine(int index)
		{
			if (this->size == 1)
			{
				return false;
			}
			//�������ҳ�ڵ�����С�ڹ涨�����������32��֮һ����ϲ�����ҳ
			else if (this->pages[index]->size() < (this->max_nodenum_per_page / 64))
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
