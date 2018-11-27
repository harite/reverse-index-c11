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
		void set(int64 key,int offset,int length)
		{
			this->key = key;
			this->offset = offset;
			this->length = length;
		}
	};
	class page
	{
	private:
		//Ԫ�ظ���
		int nodesize{ 0 };
		//Ԫ�ؿռ��ܳ���
		int nodelength{ 1024 };
		//�����Ѿ���ɾ��
		int deldatasize{ 0 };
		//�Ѿ�ʹ�õ�����
		int useddatasize{ 0 };
		//���ݿռ��ܳ���
		int datalength{ 1024 * 128 };
		//node�ڵ�
		node* nodes;
		//���ݴ�ſռ�
		char* datas;
	public:
		page()
		{
			this->datas = new char[datalength];
			this->nodes = new node[nodelength];
		}
		~page()
		{
			delete[] this->nodes;
		}
		inline int compare(int64 key1, int64 key2)
		{
			return key1 == key2 ? 0 : key1 > key2 ? 1 : -1;
		}

		void ensureCapacity(int nodeCapacity,int dataCapacity)
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
				int usedCapacity = this->useddatasize - this->deldatasize;
				int extcapacity = usedCapacity >> 1;
				this->nodelength = usedCapacity + (extcapacity > 64*1024 ? 64 * 1024 : extcapacity);
				if (this->nodelength < usedCapacity + dataCapacity)
				{
					this->nodelength = usedCapacity + dataCapacity;
				}
				char* tempdata = new char[this->nodelength];
				//�����������ڴ����������
				if (this->deldatasize > 0)
				{
					this->deldatasize = 0;
					this->useddatasize = 0;
					for (int i = 0; i < nodesize; i++)
					{
						memcpy(tempdata + useddatasize,datas+ nodes[i].offset, sizeof(char) * this->nodes[i].length);
						useddatasize += this->nodes[i].length;
					}
				}
				else
				{
					memcpy(tempdata, datas, sizeof(char) * this->useddatasize);
				}
				delete[] this->datas;
				this->datas = tempdata;
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
				ensureCapacity(1,length);
				this->nodes[this->nodesize].set(key,this->useddatasize,length);
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
