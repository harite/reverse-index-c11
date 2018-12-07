/*
* dicmapi2i.h
*  弹性扩容整形键值对，主要用于转化 64位整形到32位整形
*  Created on: 2015年2月11日
*      Author: jkuang
*/
#ifndef ELASTICSMAP_I2I_H_
#define ELASTICSMAP_I2I_H_
#include <string.h>
#include <atomic>
#include <string>
#include "classdef.h"
#include "rwsyslock.h"
using namespace std;
namespace mapi2i
{
	typedef long long int64;

	template<class k, class v> class node
	{
	public:
		k key;
		v value;
		node() = default;
		~node() = default;
		void set(k key, v value)
		{
			this->key = key;
			this->value = value;
		}
	};

	template<class k, class v> class page
	{
	private:
	
		int length{ 0 };
		node<k, v>* nodes{ null };
		int indexof(node<k, v>* nodes,int size,k key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				if (nodes[mid].key < key)
					fromIndex = mid + 1;
				else if (nodes[mid].key > key)
					toIndex = mid - 1;
				else
					return _type == qstardb::type_insert ? -(mid + 1) : mid; // key
			}
			switch (_type)
			{
			case qstardb::type_insert:
				return fromIndex > size ? size : fromIndex;
			case qstardb::type_ceil:
				return fromIndex;
			case qstardb::type_index:
				return -(fromIndex + 1);
			default:
				return toIndex;
			}
		}

		void ensurecapacity(int minCapacity)
		{
			int oldCapacity = this->length;
			if (minCapacity > oldCapacity)
			{
				int extCapacity = oldCapacity >> 1;
				this->length = minCapacity + 1 + (extCapacity > 512 ? 512 : extCapacity);
				node<k, v>* temp = new node<k, v>[this->length];
				memmove(temp, this->nodes, sizeof(node<k, v>) * this->size);
				delete[] this->nodes;
				this->nodes = temp;
			}
		}
	
	public:
		int size{ 0 };
		page(int length)
		{
			this->length = length;
			this->nodes = new node<k, v>[this->length];
		}
	
		~page()
		{
			delete[] nodes;
		}

		bool add(k key, v value)
		{
			ensurecapacity(this->size + 1);
			if (this->size == 0 || nodes[this->size - 1].key < key)
			{
				nodes[this->size++].set(key, value);
				return true;
			}
			else
			{
				int index = indexof(this->nodes,this->size,key, qstardb::type_insert);
				if (index >= 0)
				{
					int moveNum = this->size - index;
					if (moveNum > 0)
					{
						memmove(this->nodes + index + 1, this->nodes + index, sizeof(node<k, v>) * moveNum);
					}
					nodes[index].set(key, value);
					this->size++;
					return true;
				}
				else
				{
					return false;
				}
			}
		}
		bool remove(k key, v& value)
		{
			if (this->size == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(this->nodes,this->size,key, qstardb::type_index);
				if (index >= 0)
				{
					int moveNum = size - index - 1;
					value = this->nodes[index].value;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(node<k, v>) * moveNum);
					}
					size--;
					return true;
				}
				else
				{
					return false;
				}
			}

		}
		bool contains(k key)
		{
			return indexof(this->nodes,this->size,key, qstardb::type_index) >= 0;
		}

		bool get(k key, v& value)
		{
			int index = indexof(this->nodes, this->size,key, qstardb::type_index);
			if (index >= 0)
			{
				value = nodes[index].value;
				return true;
			}
			return false;
		}

		/*判定页的范围是否包含 key*/
		template<typename t> int rangecontains(k key)
		{
			if (this->nodes[this->size - 1].key < key)
			{
				return -1;
			}
			else if (this->nodes[0].key > key) 
			{
				return 1;
			}
			return 0;
		}

		/*页发生分裂*/
		page<k,v>** splitToTwo(bool tail)
		{
			page<k, v>** pages = new page<k, v>*[2];
			int mid = (this->size * (tail ? 9 : 5)) / 10;
			pages[0] = new page<k, v>(mid + 10);
			pages[0]->size = mid;
			memmove(pages[0]->nodes, this->nodes, sizeof(node<k,v>) * mid);

			pages[1] = new page<k, v>(tail ? this->size : this->size - mid + 128);
			pages[1]->size = this->size - mid;
			memmove(pages[1]->nodes, this->nodes + mid, sizeof(node<k, v>) * pages[1]->size);
			return pages;
		}
	};

	template<class k, class v> class block 
	{
	private:
		int size{ 1 };
		page<k, v>** pages;


		inline int insertAndSplit(int index,k key,v value)
		{

			int reuslt = this->pages[index]->add(key,value);

			if (this->pages[index]->size > 1024 * 8)
			{
				page<k, v>** temp = this->pages[index]->splitToTwo(index == this->size - 1);
				page<k, v>** newpages = new page<k, v>*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(page<k, v>*)*index);
				}
				int moveNum = this->size - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(page<k, v>*) * moveNum);
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
			page<k, v>* temp0 = this->pages[index0];
			page<k, v>* temp1 = this->pages[index1];
			int size0 = temp0->size;
			int size1 = temp1->size;
			page<k, v>* newpage = new page<k, v>(this->seq, size0 + size1 + 128, this->_block);
			memmove(newpage->keys, temp0->keys, sizeof(node<k,v>)*size0);
			memmove(newpage->keys + size0, temp1->keys, sizeof(node<k, v>)*size1);
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
				memmove(this->pages + index1, this->pages + index1 + 1, sizeof(page<k, v>*)*moveNum);
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
			else if (this->pages[index]->size <  64)
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
	};

}
#endif
