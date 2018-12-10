/*
 * classdef.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef CLASSDEF_H_
#define CLASSDEF_H_
#include <string>
#include <string.h>
#include <set>
#include <map>
#include <mutex>
#include <vector>
#include <chrono>   
using namespace std;
using namespace chrono;
namespace qstardb
{
	typedef long long int64;
	typedef unsigned int uint;
	typedef unsigned char uchar;
	typedef unsigned short ushort;
	typedef set<int> set_int;

	static const int MAX_AND_SIZE = 15;
	static const int MAX_CHAR_VALUE = 64;
	static const int MAX_NODES_SIZE = 1024 * 32;
	static const int MIN_INT_VALUE = 0x80000000;
	static const int MAX_INT_VALUE = 0x7fffffff;
	static const long long MIN_VALUE = 0x8000000000000000L;
	static const long long MAX_VALUE = 0x7fffffffffffffffL;

	static const char DUMP_ERR = 0x00;
	static const char NODE_ADD = 0x0A;
	static const char FILE_EOF = 0x0E;
	static const int64 TAIL_MARK = 0x9876543210L;
	static const int64 HEAD_MARK = 0x0123454321L;

	uint _inthash(int64 key)
	{
		return key & 0xffffffff;
	}

	uint _inthash(int key)
	{
		return key & 0xffffffff;
	}
	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};
	template<class k> class arrays
	{

	public:
		arrays(int size)
		{
			this->size = size;
			this->keys = new k[size];
		}
		~arrays()
		{
			delete[] keys;
		}
		k* keys;
		uint size;

	};
	/*自定义栈，默认初始化为具有64个空间的栈*/
	template<class t> class jstack
	{
	private:
		int _size { 0 };
		int length { 64 };
		t* elements { nullptr };
	public:
		jstack()
		{
			this->elements = new t[length];
		}
		jstack(int size)
		{
			this->length = size;
			this->elements = new t[this->length];
		}
		~jstack()
		{
			delete[] this->elements;
		}
		inline void push(t element)
		{
			if (this->_size == this->length)
			{
				length += (length >> 1) + 1;
				t* temp = new t[length];
				memmove(temp, this->elements, sizeof(t) * this->_size);
				delete[] this->elements;
				this->elements = temp;
			}
			this->elements[_size++] = element;
		}
		inline int jsize()
		{
			return this->_size;
		}
		inline t pop()
		{
			return this->elements[--this->_size];
		}
		inline t top()
		{
			return this->elements[this->_size - 1];
		}
		inline void setTop(t element)
		{
			this->elements[this->_size - 1] = element;
		}

	};
	template<class t1, class t2> class element2
	{
	public:
		t1 v1 { 0 };
		t2 v2 { 0 };
		element2() = default;
		~element2() = default;
		element2(t1 v1)
		{
			this->v1 = v1;
		}
		element2(t1 v1, t2 v2)
		{
			this->set(v1, v2);
		}
		void set(t1 v1, t2 v2)
		{
			this->v1 = v1;
			this->v2 = v2;
		}
	};

	template<class t1, class t2, class t3> class element3
	{
	public:
		t1 v1 { 0 };
		t2 v2 { 0 };
		t3 v3 { 0 };
		element3() = default;
		~element3() = default;

		element3(t1 v1)
		{
			this->set(v1, 0, 0);
		}

		element3(t1 v1, t2 v2, t3 v3)
		{
			this->set(v1, v2, v3);
		}
		void set(t1 v1, t2 v2, t3 v3)
		{
			this->v1 = v1;
			this->v2 = v2;
			this->v3 = v3;
		}
	};
	template<class t> class baseiterator
	{
	public:
		const static int type_keylist = 1;
		const static int type_pageskey = 2;
		const static int type_pageslist = 3;
		const static int type_arraylist = 4;
	public:
		baseiterator() = default;
		virtual ~baseiterator() = default;
		virtual bool hasnext() = 0;
		virtual t next() = 0;
		virtual t head() = 0;
		virtual t tail() = 0;
		virtual int size() = 0;
		virtual int free() = 0;
	};

	template<class t> class arraylist: public baseiterator<t>
	{
	private:
		t* array;
		bool desc;
		int start;
		int offset;
		int length;

	public:

		arraylist(t* array, int offset, int length, bool desc)
		{
			this->array = array;
			this->length = length;
			this->desc = desc;
			this->start = offset;
			this->offset = desc ? offset + (this->length - 1) : start;
		}

		~arraylist() = default;

		inline t get(int index)
		{
			return this->array[this->start + index];
		}

		inline bool hasnext()
		{
			return this->desc ? this->offset >= start : this->offset < this->length + this->start;
		}

		inline t next()
		{
			return this->array[this->desc ? this->offset-- : this->offset++];
		}
		inline t head()
		{

			return this->desc ? this->array[this->start + this->length - 1] : this->array[this->start];
		}

		inline t tail()
		{
			return this->desc ? this->array[this->start] : this->array[this->start + this->length - 1];
		}
		inline int size()
		{
			return this->length;
		}
		int free()
		{
			return baseiterator<t>::type_arraylist;
		}
	};

	template<class t> class tarray
	{
	private:
		int _size{ 0 };
		t _array[MAX_AND_SIZE];

	public:
		tarray() = default;
		~tarray() = default;
		void addNew(t value)
		{
			this->_array[this->_size++ % MAX_AND_SIZE] = value;
		}
		void set(t value)
		{
			if (this->_size == 0)
			{
				this->_array[this->_size++ % MAX_AND_SIZE] = value;
			}
		}
		void addAND(int value)
		{
			if (this->_size == 0)
			{
				return;
			}
			else
			{
				this->addNew(value);
			}
		}

		t at(int index)
		{
			return this->_array[index % MAX_AND_SIZE];
		}
		int size()
		{
			return this->_size % MAX_AND_SIZE;
		}
		t* array()
		{
			return _array;
		}
		void reset()
		{
			this->_size = 0;
		}
		//对数组进行排序消重，元素个数从小到大排序
		inline void sortAndDistinct()
		{
			int length = this->size();
			for (int i = 0; i < length - 1; i++)
			{
				bool flag = true;
				for (int j = 0; j < length - 1 - i; j++)
				{
					if (_array[j] > _array[j + 1])
					{
						t v = _array[j];
						_array[j] = _array[j + 1];
						_array[j + 1] = v;
						flag = false;
					}
				}
				if (flag)
				{
					break;
				}
			}
			int temp = length - 1;
			for (int i = 1, j = 0; i < length; i++)
			{
				t tempvalue = _array[j];
				if (tempvalue < 0)
				{
					while (_array[temp] > 0)
					{
						if (tempvalue == -_array[temp--])
						{
							//存在 A and -A 的情况
							this->_size = 0;
							return;
						}
					}
				}
				if (_array[j] == _array[i])
				{
					this->_size--;
					continue;
				}
				else
				{
					_array[++j] = _array[i];
				}
			}
		}
	};

	class valuespool
	{
		int extCapacity{ 1024 };
		jstack<tarray<int>*>* back;
		jstack<tarray<int>*>* pool;
		std::mutex lock; // @suppress("Type cannot be resolved")
	public:
		valuespool()
		{
			this->extCapacity = 1024 * 16;
			this->back = new jstack<tarray<int>*>(10);
			this->pool = new jstack<tarray<int>*>(1024 * 128);
			this->createArray(extCapacity);
		}
		~valuespool()
		{
			delete pool;
			while (back->jsize() > 0)
			{
				delete[] back->pop();
			}
			delete back;
		}
		void createArray(int capacity)
		{
			tarray<int>* arrays = new tarray<int>[capacity];
			//用于清理时候
			this->back->push(arrays);
			for (int i = 0; i < capacity; i++)
			{
				this->pool->push(arrays + i);
			}
		}
		/**/
		inline void recycle(tarray<int>* _array)
		{

			lock.lock(); // @suppress("Method cannot be resolved")
			_array->reset();
			this->pool->push(_array);
			lock.unlock(); // @suppress("Method cannot be resolved")
		}

		/*用于申请大小小于7的int数组块*/
		inline tarray<int>* get()
		{
			tarray<int>* temp;
			lock.lock(); // @suppress("Method cannot be resolved")
			if (pool->jsize() == 0)
			{
				createArray(extCapacity);
			}
			temp = pool->pop();
			lock.unlock(); // @suppress("Method cannot be resolved")
			return temp;
		}
	};
}
#endif 
