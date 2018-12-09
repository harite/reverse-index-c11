#ifndef adDICHASHSET_H_
#define adDICHASHSET_H_
#include <atomic>
#include <string.h>
#include "commons.h"
#include "filestream.h"
#include "rwsyslock.h"
using namespace std;
/*完成基础存储查询测试*/
namespace elasticset
{
	typedef long long int64;
	typedef unsigned int uint;
	template<class k> class page
	{
	public:
		k* keys;
		int size;
	private:
		int length;
		int indexof(k key, qstardb::type _type)
		{
			int fromIndex = 0;
			int toIndex = this->size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;

				if (keys[mid] < key)
					fromIndex = mid + 1;
				else if (keys[mid] > key)
					toIndex = mid - 1;
				else
					return _type == qstardb::type_insert ? -(mid + 1) : mid; // key
			}
			switch (_type)
			{
				case qstardb::type_insert:
					return fromIndex > this->size ? this->size : fromIndex;
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
				int newCapacity = minCapacity + 1 + (extCapacity > 1024 ? 1024 : extCapacity);
				k* temp = new k[newCapacity];
				memmove(temp, this->keys, sizeof(k) * this->size);
				delete[] this->keys;
				this->keys = temp;
				this->length = newCapacity;
			}
		}
	public:
		page(int length)
		{
			this->size = 0;
			this->length = length;
			this->keys = new k[this->length];
		}
		~page()
		{
			delete[] keys;
		}

		bool add(k key)
		{
			ensurecapacity(this->size + 1);
			if (this->size == 0 || keys[this->size - 1] < key)
			{
				keys[this->size++] = key;
				return true;
			}
			else
			{
				int index = indexof(key, qstardb::type_insert);
				if (index >= 0)
				{
					int moveNum = this->size - index;
					if (moveNum > 0)
					{
						memmove(this->keys + index + 1, this->keys + index, sizeof(k) * moveNum);
					}
					keys[index] = key;
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
			if (size == 0)
			{
				return false;
			}
			else
			{
				int index = indexof(key, qstardb::type_index);
				if (index >= 0)
				{
					int moveNum = size - index - 1;
					if (moveNum > 0)
					{
						memmove(this->keys + index, this->keys + index + 1, sizeof(k) * moveNum);
						this->size--;
					}
					else
					{
						this->keys[--size] = 0;
					}
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
			return indexof(key, qstardb::type_index) >= 0;
		}


		/**判定页的范围是否包含 key**/
		int rangecontains(k key)
		{
			if (this->keys[this->size - 1] < key)
			{
				return -1;
			}
			else if (this->keys[0] > key)
			{
				return 1;
			}
			return 0;
		}

		/**页发生分裂**/
		page<k>** splitToTwo(bool tail)
		{
			page<k>** pages = new page<k>*[2];
			int mid = (this->size * (tail ? 9 : 5)) / 10;
			pages[0] = new page<k>(mid + 10);
			pages[0]->size = mid;
			memmove(pages[0]->keys, this->keys, sizeof(k) * mid);

			pages[1] = new page<k>(tail ? this->size : this->size - mid + 128);
			pages[1]->size = this->size - mid;
			memmove(pages[1]->keys, this->keys + mid, sizeof(k) * pages[1]->size);
			return pages;
		}
	};
	template<class k> class block 
	{
	private:
		page<k>** pages;
		atomic<int> size{1};
		qstardb::rwsyslock lock;
	public:
		block()
		{
			this->pages = new page<k>*[this->size];
			for (int i = 0; i < this->size; i++)
			{
				this->pages[i] = new page<k>(16);
			}
		}
		~block() 
		{
			for (int i = 0; i < this->size; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
		}

		inline int indexOf(page<k>** pages, int size, k key, type _type)
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

		inline bool insertAndSplit(int index, k key)
		{

			bool reuslt = this->pages[index]->add(key);

			if (this->pages[index]->size > 1024 * 8)
			{
				page<k>** temp = this->pages[index]->splitToTwo(index == this->size - 1);
				page<k>** newpages = new page<k>*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(page<k>*)*index);
				}
				int moveNum = this->size - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(page<k>*) * moveNum);
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
			page<k>* temp0 = this->pages[index0];
			page<k>* temp1 = this->pages[index1];
			int size0 = temp0->size;
			int size1 = temp1->size;
			page<k>* newpage = new page<k>(size0 + size1 + 128);
			memmove(newpage->keys, temp0->keys, sizeof(k)*size0);
			memmove(newpage->keys + size0, temp1->keys, sizeof(k)*size1);
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
				memmove(this->pages + index1, this->pages + index1 + 1, sizeof(page<k>*)*moveNum);
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
			else if (this->pages[index]->size < 64)
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
		int add(k key)
		{
			bool result = false;
			lock.wrlock();
			if (this->size == 1 || this->pages[0]->rangecontains(key) >= 0)
			{
				result = this->insertAndSplit(0, key);
			}
			else if (this->pages[this->size - 1]->rangecontains(key) <= 0)
			{
				result = this->insertAndSplit(this->size - 1, key);
			}
			else
			{
				int pageno = indexOf(pages, this->size, key, type_ceil);
				result = this->insertAndSplit(pageno, key);
			}
			lock.unwrlock();
			return result;
		}

		bool contains(k key)
		{
			lock.rdlock();
			int pageno = indexOf(this->pages, this->size, key, type_index);
			if (pageno >= 0) {
				bool result= this->pages[pageno]->contains(key);
				lock.unrdlock();
				return result;
			}
			else {
				lock.unrdlock();
				return false;
			}
		}

		bool remove(k key)
		{
			bool result = false;
			lock.wrlock();
			if (this->pages[this->size - 1]->rangecontains(key) == 0)
			{
				result = this->pages[this->size - 1]->remove(key);
				this->combine(this->size - 1);
			}
			else
			{
				int pageno = indexOf(this->pages, this->size, key, type_index);
				if (pageno >= 0)
				{
					result = this->pages[pageno]->remove(key);
					this->combine(pageno);
				}
			}
			lock.unwrlock();
			return result;
		}
		
	};
	template<class k> class keyset
	{
	private:
		int partition;
		block<k>* blocks;
		atomic<int> size;
		int index(k key)
		{
			int value = key % partition;
			if (value < 0)
			{
				return -value;
			}
			else
			{
				return value;
			}
		}
	public:
		keyset(int part)
		{
			this->size = 0;
			this->partition = part;
			this->blocks = new block<k> [this->partition];
		}
		~keyset()
		{
			delete[] this->blocks;
		}

		void add(k key)
		{
			if (blocks[index(key)].add(key))
			{
				this->size++;
			}
		}
		void remove(k key)
		{
			if (blocks[index(key)].remove(key))
			{
				this->size--;
			}
		}

		bool contains(k key)
		{
			return blocks[index(key)].contains(key);
		}

		int keysize()
		{
			return this->size;
		}
	};
}
#endif