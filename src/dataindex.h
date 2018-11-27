/*
 * dataindex.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef DATAINDEX_H_
#define DATAINDEX_H_
#include "dxtemplate.h"
namespace qstardb
{
	/*倒排索引页结构，页由 数组length,size,keys组成*/
	template<class c> class indexpage
	{
	private:
		int length;
		void ensureCapacity(int minCapacity)
		{
			int oldCapacity = this->length;
			if (minCapacity > oldCapacity)
			{
				int extcapacity = oldCapacity >> 1;
				this->length = oldCapacity + (extcapacity > 256 ? 256 : extcapacity) + 32;
				if (this->length < minCapacity)
				{
					this->length = minCapacity;
				}
				uint* temp = new uint[this->length];
				memcpy(temp, keys, sizeof(uint) * this->size);
				delete[] this->keys;
				this->keys = temp;
			}
		}
	public:

		int size { 0 };
		uint* keys;
		bool compres { false };
		indexpage(int capacity)
		{
			this->length = capacity;
			this->keys = new uint[capacity];
		}

		~indexpage(void)
		{
			delete[] this->keys;
		}

		inline int compare(memstore<c>* store, uint k1, datanode* k2)
		{
			return store->pageno(k1)->get(k1)->compare(k2);
		}

		inline int compare(memstore<c>* store, uint k1, uint k2)
		{
			return store->pageno(k1)->get(k1)->compare(store->pageno(k2)->get(k2));
		}
		/*往页中插入数据*/
		bool insert(memstore<c>* store, uint key)
		{

			if (this->size == 0 || this->compare(store, key, keys[this->size - 1]) > 0)
			{
				ensureCapacity(this->size + 1); // Increments modCount!!
				this->keys[this->size++] = key;
				return true;
			}
			else if (this->compare(store, key, keys[0]) < 0)
			{
				ensureCapacity(this->size + 1);
				memmove(this->keys + 1, this->keys, sizeof(uint) * this->size);
				this->keys[0] = key;
				this->size++;
				return true;
			}
			else
			{
				int pos = indexof(this, this->keys, key, store, this->size, type_insert);
				ensureCapacity(this->size + 1);
				int numMoved = this->size - pos;
				if (numMoved > 0)
				{
					memmove(this->keys + pos + 1, this->keys + pos, sizeof(uint) * numMoved);
				}
				this->keys[pos] = key;
				this->size++;
				return true;
			}
		}
		/*从页中删除数据**/
		bool remove(memstore<c>* store, uint key)
		{
			int pos = indexof(this, this->keys, key, store, this->size, type_index);
			if (pos >= 0)
			{
				int numMoved = size - pos - 1;
				if (numMoved > 0)
				{
					memmove(this->keys + pos, this->keys + pos + 1, sizeof(uint) * numMoved);
				}
				this->size--;
				return true;
			}
			return false;
		}
		/*查找id在页中是否存在**/
		inline bool contains(memstore<c>* store, uint key)
		{
			return indexof(this, this->keys, key, store, this->size, type_index) >= 0;
		}
		/*判定页的范围是否包含 key*/
		template<typename t> int rangecontains(memstore<c>* store, t key)
		{
			if (this->compare(store, this->keys[this->size - 1], key) < 0)
			{
				return -1;
			}
			else if (this->compare(store, this->keys[0], key) > 0)
			{
				return 1;
			}
			return 0;
		}
		/*统计符合条件的数据总数*/
		template<typename t> int count(memstore<c>* store, t _s, t _e)
		{
			int from = indexof(this, this->keys, _s, store, this->size, type_ceil);
			int to = indexof(this, this->keys, _e, store, this->size, type_floor);
			return to - from + 1;
		}

		inline arraylist<uint>* nodelist(bool desc)
		{
			return new arraylist<uint>(this->keys, 0, this->size, desc);
		}

		inline arraylist<uint>* nodelist(memstore<c>* store, bool desc, datanode* _s_start, datanode* _e_end)
		{
			int from = indexof(this, this->keys, _s_start, store, this->size, type_ceil);
			int to = indexof(this, this->keys, _e_end, store, this->size, type_floor);
			return new arraylist<uint>(this->keys, from, to - from + 1, desc);
		}
		/*页发生分裂*/
		indexpage<c>** splitToTwo(bool tail)
		{
			indexpage<c>** pages = new indexpage<c>*[2];

			int mid = (this->size * (tail ? 9 : 5)) / 10;
			pages[0] = new indexpage<c>(mid + 10);
			pages[0]->size = mid;
			memmove(pages[0]->keys, this->keys, sizeof(uint) * mid);

			pages[1] = new indexpage<c>(tail ? this->size : this->size - mid + 10);
			pages[1]->size = this->size - mid;
			memmove(pages[1]->keys, this->keys + mid, sizeof(uint) * pages[1]->size);
			return pages;
		}
		int pagesize()
		{
			return this->size;
		}
	};

	/*当数据量较少时候，每个条件都创建独立的 索引页结构较为耗费资源，
	 *顾采用处于同一区段内数据量较小的索引条件共用一个索引结构*/
	template<class c> class memkeys
	{

	private:
		arrays<uint>* buffer;
		// size,length
		twouint nodesps;
		uint buffersize { 0 };
		// offset, lenght, size
		// first , second ,third
		threeiss* nodes;
		// size , lenght,dllsize
		threeiii property;

	public:
		uint* keys;
		memkeys(int nodesize, arrays<uint>* buffer)
		{
			this->keys = new uint[nodesize * 2];
			this->property.v2 = nodesize * 2;
			this->nodes = new threeiss[nodesize];
			this->nodesps.v2 = nodesize;
			this->buffer = buffer;
		}
		~memkeys()
		{
			delete[] this->keys;
			delete[] this->nodes;
		}

		inline int compare(memstore<c>* store, uint k1, uint k2)
		{
			return store->get(k1)->compare(store->get(k2));
		}

		threeiss& nodeat(int index)
		{
			return nodes[index];
		}

		bool release(uint index)
		{
			int nodesize = this->nodes[index].v2;
			if (nodesize > 0)
			{
				this->property.v3 += nodesize;
				this->nodes[index].v1 = 0;
				this->nodes[index].v3 = 0;
				this->nodes[index].v2 = -nodesize;
				return true;
			}
			else
			{
				return false;
			}
		}

		bool insert(uint index, memstore<c>* store, uint key)
		{
			threeiss& node = nodes[index];
			if (node.v2 <= 0)
			{
				uint temp[1];
				temp[0] = key;
				this->keys = datacp(index, keys, temp, 1, nodes, nodesps, property, false);
				node.v3++;
				return true;
			}
			else if (node.v3 == 0)
			{
				this->keys[node.v1] = key;
				node.v3++;
				return true;
			}
			else if (this->compare(store, key, keys[node.v1 + node.v3 - 1]) > 0)
			{
				ensureCapacity(index);
				this->keys[node.v1 + node.v3] = key;
				node.v3++;
				return true;
			}
			else
			{
				int pos = indexof(this, this->keys + node.v1, key, store, node.v3, type_insert);
				if (pos >= 0)
				{
					ensureCapacity(index);
					int numMoved = node.v3 - pos;
					if (numMoved > 0)
					{
						memmove(this->keys + node.v1 + pos + 1, this->keys + pos + node.v1, sizeof(uint) * numMoved);
					}
					this->keys[node.v1 + pos] = key;
					node.v3++;
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		bool ensureCapacity(int index)
		{
			if (nodes[index].v2 > nodes[index].v3)
			{
				return true;
			}
			else
			{
				int oldlen = nodes[index].v2;
				int extcapacity = (oldlen >> 1) + 4;
				extcapacity = extcapacity > 32 ? 32 : extcapacity;
				int newlen = nodes[index].v2 + extcapacity;
				uint temp[MAX_CHAR_VALUE];
				memmove(temp, keys + nodes[index].v1, sizeof(uint) * oldlen);
				property.v3 += nodes[index].v2;
				nodes[index].v2 *= -1;
				nodesps.v1--;
				if (newlen >= MAX_CHAR_VALUE)
				{
					newlen = MAX_CHAR_VALUE;
				}
				if (property.v1 - property.v3 < buffer->size && property.v1 - property.v3 + newlen < property.v2)
				{
					cleanup(buffer->keys, keys, nodes, nodesps.v2, property);
					memcpy(keys, buffer->keys, sizeof(uint) * property.v1);
				}
				this->keys = datacp(index, keys, temp, newlen, nodes, nodesps, property, false);
				return true;
			}
		}

		bool remove(uint index, memstore<c>* store, uint key)
		{
			threeiss& node = nodes[index];
			if (node.v3 <= 0 || node.v2 <= 0)
			{
				return false;
			}
			else
			{
				int pos = indexof(this, this->keys + node.v1, key, store, node.v3, type_index);
				if (pos >= 0)
				{
					int numMoved = node.v3 - pos - 1;
					if (numMoved > 0)
					{
						memmove(this->keys + pos + node.v1, this->keys + node.v1 + pos + 1, sizeof(uint) * numMoved);
					}
					if (--node.v3 == 0)
					{
						this->property.v3 += node.v2;
						node.v2 = -node.v2;
						return true;
					}
				}
				return false;
			}
		}

	};

	template<class c> class keylist: public baseiterator<arraylist<uint>*>
	{
	public:
		int length { 0 };
		arraylist<uint>* list { null };

		keylist(memstore<c>* store, uint* keys, bool desc, int _s_index, int size)
		{
			if (size > 0)
			{
				this->length = size;
				this->list = new arraylist<uint>(keys, _s_index, size, desc);
			}
		}

		~keylist()
		{
			free();
		}

		int free()
		{
			if (this->list != null)
			{
				delete this->list;
				this->list = null;
			}
			return type_keylist;
		}

		bool hasnext()
		{
			return list != null && list->hasnext();
		}

		arraylist<uint>* next()
		{
			arraylist<uint>* temp = list;
			this->list = null;
			return temp;
		}

		arraylist<uint>* head()
		{
			return list;
		}

		arraylist<uint>* tail()
		{
			return list;
		}
		inline int size()
		{
			return this->length;
		}
	};

	template<class c> class pageslist: public baseiterator<arraylist<uint>*>
	{
	public:
		bool desc { true };
		int length { 0 };
		int64 _s_sort_, _e_sort_;
		memstore<c>* store { null };
		baseiterator<indexpage<c>*>* list { null };
		pageslist(memstore<c>* store, indexpage<c>** pages, int64 _s_sort_, int64 _e_sort_, bool desc, int _s_index, int size)
		{

			this->_s_sort_ = _s_sort_;
			this->_e_sort_ = _e_sort_;
			if (size > 0)
			{
				this->length = length;
				this->list = new arraylist<indexpage<c>*>(pages, _s_index, size, desc);
			}
			this->desc = desc;
			this->store = store;
		}

		~pageslist()
		{
			free();
		}

		int free()
		{
			if (this->list != null)
			{
				delete list;
				this->list = null;
			}
			return type_pageslist;
		}

		bool hasnext()
		{
			return this->list != null && this->list->hasnext();
		}

		arraylist<uint>* next()
		{
			return get(this->list->next());
		}

		arraylist<uint>* head()
		{
			return get(this->list->head());
		}

		arraylist<uint>* tail()
		{
			return get(this->list->tail());
		}
		arraylist<uint>* get(indexpage<c>* page)
		{
			datanode s_temp(MIN_VALUE, _s_sort_);
			datanode e_temp(MAX_VALUE, _e_sort_);
			return page->nodelist(this->store, desc, &s_temp, &e_temp);
		}

		inline int size()
		{
			return this->length;
		}
	};
	template<class c> class pageskey: public baseiterator<uint>
	{

	public:
		int length { 0 };
		bool desc { true };
		datanode* s_temp, *e_temp;
		memstore<c>* store { null };
		arraylist<uint>* keys { null };
		arraylist<indexpage<c>*>* list { null };
		pageskey(memstore<c>* store, indexpage<c>** pages, datanode* _s_sort_, datanode* _e_sort_, bool desc, int _s_index, int size)
		{
			this->desc = desc;
			this->store = store;
			this->s_temp = _s_sort_;
			this->e_temp = _e_sort_;
			if (size > 0)
			{
				this->length = size;
				this->list = new arraylist<indexpage<c>*>(pages, _s_index, size, desc);
			}
		}

		~pageskey()
		{
			free();
		}

		int free()
		{
			if (this->list != null)
			{
				delete this->list;
				this->list = null;
			}
			if (this->keys != null)
			{
				delete this->keys;
				this->keys = null;
			}
			return type_pageskey;
		}

		bool hasnext()
		{
			while (this->keys == null || !this->keys->hasnext())
			{
				if (this->keys != null)
				{
					delete this->keys;
					this->keys = null;
				}
				if (this->list != null && this->list->hasnext())
				{
					this->keys = get(this->list->next());
				}
				else
				{
					return false;
				}
			}
			return true;
		}

		uint next()
		{
			return this->keys->next();
		}

		uint head()
		{
			return this->keys->get(desc ? this->keys->size() - 1 : 0);
		}

		uint tail()
		{
			return this->keys->get(desc ? 0 : this->keys->size() - 1);
		}
		arraylist<uint>* get(indexpage<c>* page)
		{
			return page->nodelist(this->store, desc, s_temp, e_temp);
		}

		inline int size()
		{
			return this->length;
		}
	};
	/*索引结构，每个具体的条件分配一个索引结构，索引内由 索引页组成*/
	template<class c> class indexseg
	{
	private:
		int offset { 0 };
		indexpage<c>** pages;
	public:
		indexseg()
		{
			this->pages = null;
		}
		~indexseg(void)
		{
			if (this->pages != null)
			{
				for (int i = 0; i < offset; i++)
				{
					delete pages[i];
				}
				delete[] pages;
			}
		}

		inline int compare(memstore<c>* store, uint k1, datanode* node)
		{
			return store->pageno(k1)->get(k1)->compare(node);
		}

		inline int compare(memstore<c>* store, uint k1, uint k2)
		{
			return store->pageno(k1)->get(k1)->compare(store->pageno(k2)->get(k2));
		}

		template<typename t> int compare(memstore<c>* store, indexpage<c>* page, t node)
		{
			return page->rangecontains(store, node);
		}

		bool insert(uint pos, memstore<c>* store, memkeys<c>* mkeys, uint key)
		{
			if (this->pages == null)
			{
				return insertbuffers(pos, store, mkeys, key);
			}
			else
			{
				return insertpage(pos, store, mkeys, key);
			}
		}
		bool insertbuffers(uint pos, memstore<c>* store, memkeys<c>* mkeys, uint key)
		{
			threeiss& node = mkeys->nodeat(pos);
			short oldlen = node.v3;
			if (oldlen < MAX_CHAR_VALUE)
			{
				mkeys->insert(pos, store, key);
				return oldlen != node.v3;
			}
			else
			{
				threeiss& node = mkeys->nodeat(pos);
				uint* temp = &mkeys->keys[node.v1];
				int index = indexof(this, temp, key, store, node.v3, type_insert);
				if (index >= 0)
				{
					this->pages = new indexpage<c>*[1];
					this->pages[0] = new indexpage<c>(MAX_CHAR_VALUE * 2);
					this->pages[0]->size = node.v3;
					memcpy(this->pages[0]->keys, temp, sizeof(uint) * this->pages[0]->size);
					this->pages[0]->insert(store, key);
					this->offset = 1;
					mkeys->release(pos);
					return true;
				}
				return false;
			}
		}

		bool insertpage(int pos, memstore<c>* store, memkeys<c>* mkeys, uint key)
		{
			if (this->offset == 0)
			{
				if (this->pages != null)
				{
					delete[] this->pages;
					this->pages = null;
					return this->insertbuffers(pos, store, mkeys, key);
				}
			}
			else if (this->pages[this->offset - 1]->rangecontains(store, key) <= 0)
			{
				if (this->pages[this->offset - 1]->insert(store, key))
				{
					split(this->offset - 1);
					return true;
				}
			}
			else if (this->pages[0]->rangecontains(store, key) >= 0)
			{
				if (this->pages[0]->insert(store, key))
				{
					split(0);
					return false;
				}
			}
			else
			{
				int pageno = indexof(this, this->pages, key, store, this->offset, type_ceil);
				if (this->pages[pageno]->insert(store, key))
				{
					split(pageno);
					return false;
				}
			}
			return false;
		}

		bool remove(uint pos, memstore<c>* store, memkeys<c>* mkeys, uint key)
		{
			if (this->pages == null)
			{
				return mkeys->remove(pos, store, key);
			}
			else
			{
				return this->remove(store, key);
			}
		}

		bool remove(memstore<c>* store, uint key)
		{
			if (this->offset == 0)
			{
				return true;
			}
			else if (this->pages[this->offset - 1]->rangecontains(store, key) == 0)
			{
				if (this->pages[this->offset - 1]->remove(store, key))
				{
					combine(store, offset - 1);
				}
			}
			else
			{
				int pageno = indexof(this, this->pages, key, store, this->offset, type_index);
				if (pageno >= 0)
				{
					if (this->pages[pageno]->remove(store, key))
					{
						combine(store, pageno);
					}
				}
			}
			return this->offset == 0;
		}

		bool combine(memstore<c>* store, int index)
		{
			if (this->pages[index]->pagesize() == 0)
			{
				int move = this->offset - index - 1;
				delete pages[index];
				if (move > 0)
				{
					memmove(this->pages + index, this->pages + index + 1, sizeof(indexpage<c>*) * move);
				}
				pages[--this->offset] = null;
				if (this->offset == 0)
				{
					delete[] this->pages;
					this->pages = null;
				}
				return true;
			}
			else if (index + 1 < this->offset && this->pages[index]->pagesize() < 255)
			{
				int pos = index > 0 ? index : 1;
				int size = this->pages[pos]->pagesize();
				uint* keys = this->pages[pos]->keys;
				for (int i = 0; i < size; i++)
				{
					this->pages[pos - 1]->insert(store, keys[i]);
				}
				int move = this->offset - pos - 1;
				delete pages[pos];
				if (move > 0)
				{
					memmove(this->pages + pos, this->pages + pos + 1, sizeof(indexpage<c>*) * move);
				}
				pages[--this->offset] = null;
			}
			return false;
		}

		void split(uint index)
		{
			if (this->pages[index]->pagesize() > MAX_NODES_SIZE)
			{
				indexpage<c>** temp = pages[index]->splitToTwo(index + 1 == this->offset && this->offset > 1);
				indexpage<c>** newpages = new indexpage<c>*[this->offset + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(indexpage<c>*) * index);
				}
				int moveNum = this->offset - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(indexpage<c>*) * moveNum);
				}
				newpages[index] = temp[0];
				newpages[index + 1] = temp[1];
				delete pages[index];
				delete[] temp;
				delete[] this->pages;
				this->pages = newpages;
				this->offset++;
			}
		}

		baseiterator<arraylist<uint>*>* idslist(uint pos, memstore<c>* store, memkeys<c>* mkeys, int64 _s_sort_, int64 _e_sort_, bool desc)
		{
			if (_s_sort_ == 0 && _e_sort_ == 0)
			{
				_s_sort_ = MIN_VALUE;
				_e_sort_ = MAX_VALUE;
			}
			datanode s_temp(MIN_VALUE, _s_sort_);
			datanode e_temp(MAX_VALUE, _e_sort_);
			if (this->pages != null)
			{
				int _s_index = indexof(this, pages, &s_temp, store, this->offset, type_ceil);
				int _e_index = indexof(this, pages, &e_temp, store, this->offset, type_floor);
				return new pageslist<c>(store, pages, _s_sort_, _e_sort_, desc, _s_index, _e_index - _s_index + 1);
			}
			else
			{
				threeiss& node = mkeys->nodeat(pos);
				int _s_index = indexof(this, mkeys->keys + node.v1, &s_temp, store, node.v3, type_ceil);
				int _e_index = indexof(this, mkeys->keys + node.v1, &e_temp, store, node.v3, type_floor);
				return new keylist<c>(store, mkeys->keys + node.v1, desc, _s_index, _e_index - _s_index + 1);
			}
		}

		baseiterator<uint>* keys(uint pos, memstore<c>* store, memkeys<c>* mkeys, datanode* s_temp, datanode* e_temp, bool desc)
		{

			if (this->pages != null)
			{
				int _s_index = indexof(this, pages, s_temp, store, this->offset, type_ceil);
				int _e_index = indexof(this, pages, e_temp, store, this->offset, type_floor);
				return new pageskey<c>(store, pages, s_temp, e_temp, desc, _s_index, _e_index - _s_index + 1);
			}
			else
			{
				threeiss& node = mkeys->nodeat(pos);
				int _s_index = indexof(this, mkeys->keys + node.v1, s_temp, store, node.v3, type_ceil);
				int _e_index = indexof(this, mkeys->keys + node.v1, e_temp, store, node.v3, type_floor);
				return new arraylist<uint>(mkeys->keys + node.v1, _s_index, _e_index - _s_index + 1, desc);
			}
		}

		int scansize(uint pos, memstore<c>* store, memkeys<c>* mkeys, int64 _s_sort_, int64 _e_sort_)
		{
			if (_s_sort_ > _e_sort_)
			{
				return 0;
			}
			else if (_s_sort_ == 0 && _e_sort_ == 0)
			{
				if (this->pages == null)
				{
					threeiss& node = mkeys->nodeat(pos);
					return node.v3;
				}
				int sum = 0;
				for (int i = 0; i < this->offset; i++)
				{
					sum += this->pages[i]->pagesize();
				}
				return sum;
			}
			else
			{
				datanode s_temp(MIN_VALUE, _s_sort_);
				datanode e_temp(MAX_VALUE, _e_sort_);
				return scansize(pos, store, mkeys, s_temp, e_temp);
			}
		}

		int scansize(uint pos, memstore<c>* store, memkeys<c>* mkeys, datanode& s_temp, datanode& e_temp)
		{
			if (this->pages == null)
			{
				threeiss& node = mkeys->nodeat(pos);
				int _s_index = indexof(this, mkeys->keys + node.v1, &s_temp, store, node.v3, type_ceil);
				int _e_index = indexof(this, mkeys->keys + node.v1, &e_temp, store, node.v3, type_floor);
				return _e_index - _s_index + 1;
			}
			else
			{
				int _s_index = indexof(this, this->pages, &s_temp, store, this->offset, type_ceil);
				int _e_index = indexof(this, this->pages, &e_temp, store, this->offset, type_floor);
				int sum = 0;
				for (int i = _s_index; i <= _e_index; i++)
				{
					if (i == _s_index || i == _e_index)
					{
						sum += this->pages[i]->count(store, &s_temp, &e_temp);
					}
					else
					{
						sum += this->pages[i]->pagesize();
					}
				}
				return sum;
			}
		}

	};
	template<class c> class segments
	{
		//用于索引的扩展
		static const uint column = 1024 * 4;
	private:
		bool hassegment { false };
		uint length { 0 };
		arrays<uint>* buffer;
		memstore<c>* store;
		memkeys<c>** mkeys;
		indexseg<c>** segs;

		inline uint xpos(uint id)
		{
			return id / column;
		}
		inline uint ypos(uint id)
		{
			return id % column;
		}
	public:
		segments(memstore<c>* store)
		{
			this->mkeys = null;
			this->segs = null;
			this->store = store;
			this->hassegment = false;
			this->buffer = new arrays<uint>(1024 * 1024);
		}
		~segments()
		{
			if (buffer != null)
			{
				delete buffer;
			}
			if (this->mkeys != null)
			{
				for (uint i = 0; i < length; i++)
				{
					delete this->mkeys[i];
				}
				delete[] this->mkeys;
			}
			if (this->segs != null)
			{
				for (uint i = 0; i < length; i++)
				{
					delete[] this->segs[i];
				}
				delete[] this->segs;
			}
		}
		inline bool hasSegment()
		{
			if (hassegment)
			{
				return hassegment;
			}
			else if (this->length > 0)
			{
				this->hassegment = true;
				return true;
			}
			else
			{
				return false;
			}
		}

		inline bool insert(uint index, uint key)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			ensurecapacity(xpos);
			return this->segs[xpos][ypos].insert(ypos, store, mkeys[xpos], key);
		}

		inline baseiterator<arraylist<uint>*>* idslist(uint index, int64 _s_sort_, int64 _e_sort_, bool desc)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			return this->segs[xpos][ypos].idslist(ypos, store, mkeys[xpos], _s_sort_, _e_sort_, desc);
		}

		inline baseiterator<uint>* keys(uint index, datanode* _s_sort_, datanode* _e_sort_, bool desc)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			return this->segs[xpos][ypos].keys(ypos, store, mkeys[xpos], _s_sort_, _e_sort_, desc);
		}

		inline int scansize(uint index, int64 _s_sort_, int64 _e_sort_)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			return this->segs[xpos][ypos].scansize(ypos, store, mkeys[xpos], _s_sort_, _e_sort_);
		}

		inline int scansize(uint index, datanode& _s_sort_, datanode& _e_sort_)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			return this->segs[xpos][ypos].scansize(ypos, store, mkeys[xpos], _s_sort_, _e_sort_);
		}

		inline void ensurecapacity(uint rowid)
		{
			if (rowid + 1 > length)
			{
				this->segs = encapacity(this->segs, this->length, rowid + 1, this->column);
				memkeys<c>** temp = new memkeys<c>*[rowid + 1];
				if (length > 0 && mkeys != null)
				{
					memcpy(temp, mkeys, sizeof(memkeys<c>*) * length);
				}
				for (uint i = length; i < rowid + 1; i++)
				{
					temp[i] = new memkeys<c>(this->column, this->buffer);
				}
				if (mkeys != null)
				{
					delete[] mkeys;
				}
				this->mkeys = temp;
				this->length = rowid + 1;
			}
		}

		inline void remove(uint id, uint* ptr, int len)
		{
			for (int i = 0; i < len; i++)
			{
				this->remove(id, ptr[i]);
			}
		}

		inline void remove(uint id, uint index)
		{
			uint xpos = this->xpos(index);
			uint ypos = this->ypos(index);
			this->segs[xpos][ypos].remove(ypos, this->store, mkeys[xpos], id);
		}
	};

}
#endif
