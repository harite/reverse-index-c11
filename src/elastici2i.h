#ifndef ELASTICSMAP_I2I_H_
#define ELASTICSMAP_I2I_H_
#include <string.h>
#include <atomic>
#include <string>
#include "classdef.h"
#include "rwsyslock.h"
using namespace std;
namespace elasticsmap
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
		int length{ 0 };
		node<k, v>* nodes{ null };
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
		bool remove(k key)
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

		int rangecontains(k key)
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
	public:
		block()
		{
			this->pages = new page<k, v>*[this->size];
			for (int i = 0; i < this->size; i++)
			{
				this->pages[i] = new page<k, v>(64);
			}
		}
		~block(){
			for (int i = 0; i < this->size; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
		}
		inline int indexOf(page<k, v>** pages, int size,k key,  type _type)
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

		inline int insertAndSplit(int index,k key,v value)
		{

			int reuslt = this->pages[index]->add(key,value);

			if (this->pages[index]->size > 1024 * 4)
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
			page<k, v>* newpage = new page<k, v>( size0 + size1 + 128);
			memmove(newpage->nodes, temp0->nodes, sizeof(node<k,v>)*size0);
			memmove(newpage->nodes + size0, temp1->nodes, sizeof(node<k, v>)*size1);
			newpage->size = size0 + size1;
			delete temp0;
			delete temp1;
			this->pages[index0] = newpage;
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
			else if (this->pages[index]->size <  64)
			{
				if (index == 0)
				{
					combine(0, 1);
				}
				else
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
		int insert(k key,v value)
		{
			if (this->size == 1 || this->pages[0]->rangecontains(key) >= 0)
			{
				return this->insertAndSplit(0, key, value);
			}
			else if (this->pages[this->size - 1]->rangecontains(key) <= 0)
			{
				return this->insertAndSplit(this->size - 1, key, value);
			}
			else
			{
				int pageno = indexOf(pages, this->size, key,  type_ceil);
				return this->insertAndSplit(pageno, key, value);
			}
		}

		bool remove(k key)
		{
			if (this->pages[this->size - 1]->rangecontains(key) == 0)
			{
				bool deled = this->pages[this->size - 1]->remove(key);
				this->combine(this->size - 1);
				return deled;
			}
			else
			{
				int pageno = indexOf(this->pages, this->size, key,  type_index);
				if (pageno >= 0)
				{
					bool deled = this->pages[pageno]->remove(key);
					this->combine(pageno);
					return deled;
				}
				return false;
			}
		}
		int get(k key, v& value)
		{
			int pageno = indexOf(this->pages, this->size, key,  type_index);
			if (pageno >= 0) {
				return this->pages[pageno]->get(key, value);
			}
			else {
				return qstardb::MIN_INT_VALUE;
			}
		}
	};
	template<class k, class v> class emapi2i {
	private:
		int partition;
		block<k, v>* blocks;
		qstardb::rwsyslock rwlock;
		inline int ypos(k key)
		{
			int pos = key % partition;
			if (pos < 0) {
				return -pos;
			}
			else {
				return pos;
			}
		}
	public:
		emapi2i(int partition)
		{
			this->partition =  partition;
			this->blocks = new block<k, v>[this->partition];
		}
		~emapi2i()
		{
			delete[] this->blocks;
		}
	
		int get(k key, v& value)
		{
			int pos = ypos(key);
			rwlock.rdlock();
			int result = this->blocks[pos].get(key, value);
			rwlock.unrdlock();
			return result;
		}
		int add(k key,v value)
		{
			int pos = ypos(key);
			rwlock.wrlock();
			int result = this->blocks[pos].insert(key,value);
			rwlock.unwrlock();
			return result;
		}
		bool remove(k key)
		{
			int pos = ypos(key);
			rwlock.wrlock();
			int result = this->blocks[pos].remove(key);
			rwlock.unwrlock();
			return result;
		}
	};
}
#endif
