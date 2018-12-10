/*
 * dataquue.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  OPERATE_OR_H_
#define  OPERATE_OR_H_
#include "datastore.h"
#include "dataindex.h"
#include <mutex>    
using namespace std;
namespace qstardb
{
	template<class c> class iditerator
	{
	private:
		uint index{ 0 };
		bool desc{ true };
		bool _virtual{ true };
		stopdecoder decoder;
		datanode* data{ nullptr };
		segments<c>* segs{ nullptr };
		memstore<c>* store{ nullptr };
		treeanalysis* left{ nullptr };
		treeanalysis* right{ nullptr };
		baseiterator<uint>* nodes{ nullptr };
		baseiterator<arraylist<uint>*>* nodeslist{ nullptr };
		static const int MAX_SCAN_COUNT = 512;
	public:
		iditerator(segments<c>* segs, memstore<c>* store)
		{
			this->segs = segs;
			this->store = store;
		}

		~iditerator()
		{
			this->reset();
		}

		inline void set(baseiterator<arraylist<uint>*>* nodeslist, treepool* tnodepool, bool desc, uint index, vector<tarray<int>*>& filters)
		{
			this->desc = desc;
			this->index = index;
			this->_virtual = true;
			this->nodeslist = nodeslist;
			this->left = new treeanalysis(tnodepool, filters, index);
		}

		void reset()
		{
			if (this->nodeslist != nullptr)
			{
				this->nodeslist->free();
				delete this->nodeslist;
				this->nodeslist = nullptr;
			}
			if (this->nodes != nullptr)
			{
				this->nodes->free();
				delete this->nodes;
				this->nodes = nullptr;
			}
			if (this->left != nullptr)
			{
				delete this->left;
				this->left = nullptr;
			}
			this->data = nullptr;
			this->right = nullptr;
		}

		void setright(treeanalysis* right)
		{
			this->right = right;
		}

		inline datanode* peek()
		{
			hasnext();
			return data;
		}

		inline datanode* next()
		{
			datanode* temp = data;
			data = nullptr;
			return temp;
		}

		inline bool isVirtual()
		{
			return _virtual;
		}

		inline int compareTo(iditerator* o)
		{
			return this->peek()->compare(o->peek()) * (desc ? -1 : 1);
		}

		inline bool iterhasnext()
		{
			while (this->nodes == nullptr || !this->nodes->hasnext())
			{
				if (this->nodes != nullptr)
				{
					this->nodes->free();
					delete this->nodes;
					this->nodes = nullptr;
				}
				if (this->nodeslist->hasnext())
				{
					this->nodes = this->nodeslist->next();
					int minSize = -1;
					int minIndex = -1;
					if (this->left->shardSize > 0)
					{
						datanode* max_node = store->get(desc ? this->nodes->head() : this->nodes->tail());
						datanode* min_node = store->get(desc ? this->nodes->tail() : this->nodes->head());
						for (int i = 0; i < this->left->shardSize; i++)
						{
							uint otherIndex = this->left->shardIndex[i];
							if (otherIndex == this->index)
							{
								continue;
							}
							int size = this->segs->scansize(otherIndex, *min_node, *max_node);
							if (minSize == -1 || size < minSize)
							{
								minSize = size;
								minIndex = otherIndex;
							}
						}
						if (minIndex != -1 && minSize * 10 < this->nodes->size() * 7)
						{
							this->nodes->free();
							delete this->nodes;
							this->nodes = this->segs->keys(minIndex, min_node, max_node, desc);
						}
					}
					if (this->nodes->hasnext())
					{
						return true;
					}
				}
				else
				{
					return false;
				}
			}
			return true;
		}
		inline bool hasnext()
		{
			if (data != nullptr)
			{
				return true;
			}
			int count = 0;
			while (iterhasnext())
			{
				auto key = this->nodes->next();
				auto page = store->pageno(key);
				auto tnode = page->get(key);
				if (page->contains(tnode, left, decoder) && page->contains(tnode, right, decoder))
				{
					data = tnode;
					_virtual = false;
					return true;
				}
				else if (count++ > MAX_SCAN_COUNT)
				{
					data = tnode;
					_virtual = true;
					return true;
				}
			}
			return false;
		}
	};
	template<class t> class iditerpool
	{
		std::mutex lock;
		segments<t>* segs;
		memstore<t>* store;
		jstack<iditerator<t>*>* pool;
	public:
		iditerpool(segments<t>* segs, memstore<t>* store)
		{
			this->segs = segs;
			this->store = store;
			this->pool = new jstack<iditerator<t>*>(1024);
		}
		~iditerpool()
		{
			while (this->pool->jsize() > 0)
			{
				delete this->pool->pop();
			}
			delete this->pool;
		}
		;
		/**/
		void recycle(iditerator<t>* param)
		{

			lock.lock();
			param->reset();
			this->pool->push(param);
			lock.unlock();
		}

		/*用于申请大小小于7的int数组块*/
		iditerator<t>* get()
		{
			iditerator<t>* temp;
			lock.lock();
			// 如果池中存在空余的参数对象，则直接返回
			if (pool->jsize() == 0)
			{
				temp = new iditerator<t>(this->segs, this->store);
			}
			else
			{
				temp = pool->pop();
			}
			lock.unlock();
			return temp;
		}
	};
	template<class t> class dataqueue
	{
	private:
		treeanalysis* right;
		iditerator<t>** queue;
		iditerpool<t>* iterpool;
		int size{ 0 }, capacity, scansize{ 0 };
		datanode*tempnode{ nullptr }, *duplicate{ nullptr };

		bool ensurecapacity()
		{
			return this->size + 1 < this->capacity;
		}

		inline void siftdown(int k, iditerator<t>* key)
		{
			int half = this->size >> 1; // loop while a non-leaf
			while (k < half)
			{
				int child = (k << 1) + 1; // assume left child is least
				iditerator<t>* c = queue[child];
				int right = child + 1;
				if (right < this->size && c->compareTo(queue[right]) > 0)
					c = queue[child = right];
				if (key->compareTo(c) <= 0)
					break;
				queue[k] = c;
				k = child;
			}
			queue[k] = key;
		}

		inline void siftup(int k, iditerator<t>* x)
		{
			while (k > 0)
			{
				int parent = (k - 1) >> 1;
				iditerator<t>* e = queue[parent];
				if (x->compareTo(e) >= 0)
					break;
				queue[k] = e;
				k = parent;
			}
			queue[k] = x;
		}

		inline iditerator<t>* removeat(int i)
		{

			this->iterpool->recycle(queue[i]);
			int s = --this->size;
			if (s == i)
			{
				queue[i] = nullptr;
			}
			else
			{
				iditerator<t>* moved = queue[s];
				queue[s] = nullptr;
				siftdown(i, moved);
				if (queue[i] == moved)
				{
					siftup(i, moved);
					if (queue[i] != moved)
					{
						return moved;
					}
				}
			}
			return nullptr;
		}

	public:
		dataqueue(iditerpool<t>* iterpool, treepool* pool, int initialCapacity, vector<tarray<int>*>& filters)
		{
			this->iterpool = iterpool;
			this->capacity = initialCapacity;
			this->right = new treeanalysis(pool, filters, 0);
			this->queue = new iditerator<t>*[capacity];
		}
		~dataqueue()
		{
			for (int i = 0; i < this->size; i++)
			{
				this->iterpool->recycle(this->queue[i]);
			}
			delete[] this->queue;
			delete right;
		}

		bool additer(baseiterator<arraylist<uint>*>* nodeslist, treepool* tnodepool, bool desc, uint index, vector<tarray<int>*>& filters)
		{
			iditerator<t>* iditer = iterpool->get();
			iditer->set(nodeslist, tnodepool, desc, index, filters);
			iditer->setright(this->right);
			if (!iditer->hasnext())
			{
				this->iterpool->recycle(iditer);
				return false;
			}
			if (!this->ensurecapacity())
			{
				this->capacity += 5;
				iditerator<t>** temp = new iditerator<t>*[this->capacity];
				memmove(temp, this->queue, sizeof(iditerator<t>*) * this->size);
				delete[] this->queue;
				this->queue = temp;
			}
			if (this->size == 0)
			{
				queue[0] = iditer;
			}
			else
			{
				siftup(this->size, iditer);
			}
			this->size++;
			return true;

		}

		inline bool hasnext()
		{
			if (this->tempnode)
			{
				return true;
			}
			while (this->size > 0)
			{
				iditerator<t>* result = queue[0];
				bool _virtual = result->isVirtual();
				datanode* temp = result->next();
				if (!result->hasnext())
				{
					removeat(0);
				}
				else if (this->size > 1)
				{
					siftdown(0, result);
				}
				if (!_virtual && temp != this->duplicate)
				{
					this->tempnode = temp;
					this->duplicate = temp;
					return true;
				}
			}
			return false;
		}

		inline datanode* next()
		{
			datanode* tmpe = this->tempnode;
			this->tempnode = nullptr;
			return tmpe;
		}
	};
}
#endif
