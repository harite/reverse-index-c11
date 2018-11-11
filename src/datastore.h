/*
 * datastore.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  DATASTORE_H_
#define  DATASTORE_H_
#pragma once
#include "dxtemplate.h"
#include "stopdecoder.h"
#include <unordered_map>
using namespace std;
namespace qstardb
{

	class treenode
	{

	public:
		// 标记当前节点是否是一个词的终止字符
		bool isLeaf { false };
		// 当前节点的字符
		int value { 0 };
		// 子节点个数
		int nodesize { 0 };
		// 扩展长度
		int nodelength { 0 };
		// 子节点
		treenode* children { null };

		uint notfilterlen { 0 };

		arrays<uint>** notfilters { null };

		void set(int ch, bool isLeaf)
		{
			this->value = ch;
			this->isLeaf = isLeaf;
			this->nodesize = 0;
			this->nodelength = 0;
			this->children = null;
			this->notfilterlen = 0;
			this->notfilters = null;
		}

		/*二分查找算法*/
		int indexOf(uint node, type _type)
		{
			int fromIndex = 0;
			int toIndex = this->nodesize - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = children[mid].compareTo(node); // this.comparator.compare(nodes[mid],
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
				// found
			}
			switch (_type)
			{
				case type_insert:
					return fromIndex;
				case type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}
		/*在当前节点插入子节点,length 高16 为 notfilter size，低16位为filter size*/
		/**/

		void setnotfilter(uint* str, int size)
		{
			if (size > 0)
			{
				arrays<uint>** temp = new arrays<uint>*[this->notfilterlen + 1];
				if (this->notfilterlen >= 1)
				{
					if (this->notfilters != null)
					{
						memmove(temp, notfilters, sizeof(arrays<uint>*) * (this->notfilterlen));
						delete[] this->notfilters;
					}
				}
				this->notfilters = temp;
				this->notfilters[this->notfilterlen] = new arrays<uint>(size);
				memmove(notfilters[this->notfilterlen]->keys, str, sizeof(int) * size);
				this->notfilterlen++;
			}
		}

		inline int compareTo(treenode& o)
		{
			if (this->value > o.value)
			{
				return 1;
			}
			else if (this->value < o.value)
			{
				return -1;
			}
			return 0;
		}

		inline int compareTo(int o)
		{
			if (this->value > o)
			{
				return 1;
			}
			else if (this->value < o)
			{
				return -1;
			}
			return 0;
		}
	public:
		treenode() = default;

		~treenode() = default;

		int size()
		{
			return this->nodesize;
		}
		// 调用此函数初始化的为跟节点
		treenode(bool isLeaf)
		{
			this->isLeaf = isLeaf;
		}

		inline bool findOne(uint* chs0, int clen0, uint* chs1, int clen1)
		{
			int length0 = 0, length1 = 0;
			while (length0 < clen0 && length1 < clen1)
			{
				if (chs0[length0] < chs1[length1])
				{
					length0++;
				}
				else if (chs0[length0] > chs1[length1])
				{
					length1++;
				}
				else
				{
					return true;
				}
			}
			return false;
		}

		inline bool notfindOne(uint* chs1, int clen1)
		{
			for (uint i = 0; i < this->notfilterlen; i++)
			{
				if (!findOne(notfilters[i]->keys, notfilters[i]->size, chs1, clen1))
				{
					return true;
				}
			}
			return false;
		}

		inline bool findOne(uint* chs0, int clen0, stopdecoder& decoder)
		{
			int length0 = 0, value = -1;
			if (decoder.hasNext())
			{
				value = decoder.nextSum();
			}
			while (length0 < clen0)
			{
				if ((int) chs0[length0] < value)
				{
					length0++;
				}
				else if ((int) chs0[length0] > value)
				{
					if (decoder.hasNext())
					{
						value = decoder.nextSum();
					}
					else
					{
						return false;
					}
				}
				else
				{
					return true;
				}
			}
			return false;
		}
		/**/
		inline bool notfindOne(stopdecoder& decoder)
		{
			if (this->notfilterlen == 0)
			{
				return false;
			}
			decoder.mark();
			for (uint i = 0; i < this->notfilterlen; i++)
			{
				decoder.resetToHead();
				if (!findOne(notfilters[i]->keys, notfilters[i]->size, decoder))
				{
					decoder.reset();
					return true;
				}
			}
			decoder.reset();
			return false;
		}
		/*查找节点是否能匹配输入的顺序序列*/
		inline bool find(uint* chs, int clen, int nextPos)
		{
			uint ch0, ch1, k = 0, ksize = this->nodesize;
			if (nextPos >= clen || ksize == 0 || (ch1 = children[0].value) > chs[clen - 1] || children[ksize - 1].value < (int) (ch0 = chs[nextPos]))
			{
				return false;
			}
			while (true)
			{
				if (ch1 > ch0)
				{
					loop2: while (++nextPos < clen)
					{
						if (ch1 > (ch0 = chs[nextPos]))
						{
							if (nextPos + 10 < clen && ch1 > chs[nextPos + 10])
							{
								nextPos += 10;
								ch0 = chs[nextPos];
							}
							continue;
						}
						else if (ch1 < ch0)
						{
							goto loop3;
						}

						goto loop1;
					}
					return false;
				}
				else if (ch1 < ch0)
				{
					loop3: while (++k < ksize)
					{
						if ((ch1 = children[k].value) < ch0)
						{
							continue;
						}
						else if (ch1 > ch0)
						{
							goto loop2;
						}
						goto loop1;
					}
					return false;
				}

				loop1: if (children[k].isLeaf || notfindOne(chs, clen))
				{
					return true;
				}
				else if (++nextPos == clen)
				{
					return false;
				}
				else if (children[k].find(chs, clen, nextPos))
				{
					return true;
				}
				else if (++k == ksize)
				{
					return false;
				}
				else
				{
					ch0 = chs[nextPos];
					ch1 = children[k].value;
				}
			}
			return false;
		}
		inline bool find(stopdecoder& decoder)
		{
			uint k = 0, ch0, ksize = this->nodesize;
			if (!decoder.hasNext() || ksize == 0 || children[ksize - 1].value < (int) (ch0 = decoder.nextSum()))
			{
				return false;
			}
			uint ch1 = children[0].value;
			while (true)
			{
				if (ch1 > ch0)
				{
					loop2: while (decoder.hasNext())
					{
						if (ch1 > (ch0 = decoder.nextSum()))
						{
							continue;
						}
						else if (ch1 < ch0)
						{
							goto loop3;
						}
						goto loop1;
					}
					return false;
				}
				if (ch1 < ch0)
				{
					loop3: while (++k < ksize)
					{

						if ((ch1 = children[k].value) < ch0)
						{
							continue;
						}
						else if (ch1 > ch0)
						{
							goto loop2;
						}
						goto loop1;
					}
					return false;
				}

				loop1: if (children[k].isLeaf || notfindOne(decoder))
				{
					return true;
				}
				//迭代进度保存
				decoder.mark();
				if (children[k].find(decoder))
				{
					return true;
				}
				//还原进度
				decoder.reset();
				if (++k == ksize)
				{
					return false;
				}
				else
				{
					ch0 = decoder.nextSum();
					ch1 = children[k].value;
				}
			}
			return false;
		}
		inline bool find(uint* property, int len)
		{
			return this->find(property, len, 0);
		}
	};

	class treepool
	{
	private:
		int extCapacity;
		std::mutex lock; // @suppress("Type cannot be resolved")
		jstack<treenode*>* pool;
	public:
		treepool()
		{
			this->extCapacity = 512 * 1024;
			this->pool = new jstack<treenode*>(extCapacity);
		}
		~treepool()
		{
			while (pool->jsize() > 0)
			{
				delete[] pool->pop();
			}
			delete pool;
		}

		/**/
		void recycle(treenode* nodes)
		{

			lock.lock(); // @suppress("Method cannot be resolved")
			pool->push(nodes);
			lock.unlock(); // @suppress("Method cannot be resolved")
		}

		/*用于申请大小小于7的int数组块*/
		treenode* get(int length)
		{
			if (length <= MAX_AND_SIZE)
			{
				treenode* temp;
				lock.lock(); // @suppress("Method cannot be resolved")
				if (pool->jsize() == 0)
				{
					temp = new treenode[MAX_AND_SIZE];
				}
				else
				{
					temp = pool->pop();
				}
				lock.unlock(); // @suppress("Method cannot be resolved")
				return temp;
			}
			else
			{
				return new treenode[length];;
			}
		}
	};
	/*用于查找输入的有序序列是否至少包含指定有序序列集合中的一个*/
	class treeanalysis
	{
	public:
		int shardSize { 0 };
		int shardIndex[10];
		uint buffer[MAX_AND_SIZE];
		treepool* pool { null };
		treenode* root { null };
		~treeanalysis()
		{
			if (root != null)
			{
				this->recycle(root);
				delete root;
			}
		}

		int size()
		{
			if (root == null)
			{
				return 0;
			}
			else
			{
				return root->size();
			}
		}

		inline int cp2buffer(tarray<int>* _array, uint* buffer, int* notfilter, int masterIndex, bool removeMaster)
		{
			int size = 0;
			int notsize = 0;
			for (int i = 0, length = _array->size(); i < length; i++)
			{
				int value = _array->at(i);
				if (removeMaster && masterIndex == value)
				{
					continue;
				}
				else if (value >= 0)
				{
					buffer[size++] = value;
				}
				else
				{
					notfilter[notsize++] = -value;
				}

			}
			int filtersize = size;
			while (notsize > 0)
			{
				buffer[size++] = notfilter[--notsize];
			}
			//高16位存储非条件，低16位存储包含条件
			return (0xffff & filtersize) | (((size - filtersize) & 0xffff) << 16);;
		}

		inline void recycle(treenode* node)
		{
			if (node->notfilters != null)
			{
				for (uint i = 0; i < node->notfilterlen; i++)
				{
					delete node->notfilters[i];
				}
				delete[] node->notfilters;
			}
			for (int i = 0; i < node->nodesize; i++)
			{
				this->recycle(node->children + i);
			}
			if (node->nodelength == MAX_AND_SIZE)
			{
				this->pool->recycle(node->children);
			}
			else
			{
				delete[] node->children;
			}
		}

		inline void insert(treenode* node, uint* str, int length, int pos)
		{

			uint ch = str[pos];
			int filtersize = 0xffff & length;
			int notfiltersize = 0xffff & (length >> 16);
			bool isEnd = (pos == filtersize - 1);
			if (node->nodesize == 0)
			{
				node->nodelength = MAX_AND_SIZE;
				node->children = pool->get(node->nodelength);
				if (notfiltersize == 0 && isEnd)
				{
					node->children[0].set(ch, true);
				}
				else if (!isEnd)
				{
					node->children[0].set(ch, false);
					insert(node->children, str, length, pos + 1);
				}
				else if (notfiltersize != 0)
				{
					node->children[0].set(ch, false);
					node->setnotfilter(str + pos + 1, notfiltersize);
				}
				node->nodesize++;
			}
			else
			{
				int _index = node->indexOf(ch, type_insert);
				if (_index >= 0)
				{
					int moved = node->nodesize - _index;
					if (node->nodesize == node->nodelength)
					{
						int oldlength = node->nodelength;
						node->nodelength += 10;
						treenode* temp = new treenode[node->nodelength];
						memcpy(temp, node->children, sizeof(treenode) * node->nodesize);
						if (oldlength == MAX_AND_SIZE)
						{

							pool->recycle(node->children);
						}
						else
						{
							delete[] node->children;
						}
						node->children = temp;
					}
					if (moved > 0)
					{
						memmove(node->children + _index + 1, node->children + _index, sizeof(treenode) * moved);
					}
					if (notfiltersize == 0 && isEnd)
					{
						node->children[_index].set(ch, true);
					}
					else if (!isEnd)
					{
						node->children[_index].set(ch, false);
						insert(node->children + _index, str, length, pos + 1);
					}
					else if (notfiltersize != 0)
					{
						node->children[_index].set(ch, false);
						node->setnotfilter(str + pos + 1, notfiltersize);
					}
					node->nodesize++;
				}
				else
				{
					if (node->children[-_index - 1].isLeaf || (notfiltersize == 0 && isEnd))
					{
						node->children[-_index - 1].isLeaf = true;
					}
					else if (!isEnd)
					{
						insert(node->children - _index - 1, str, length, pos + 1);
					}
					else if (notfiltersize != 0)
					{
						node->setnotfilter(str + pos + 1, notfiltersize);
					}
				}
			}
		}
		inline void add(treenode* node, uint* chs, int len)
		{
			if ((len & 0xffff) < MAX_AND_SIZE)
			{
				insert(node, chs, len, 0);
			}
			else
			{
				cout << "error , treenode length > 32 " << endl;
			}
		}

		treeanalysis(treepool* pool, vector<tarray<int>*>& filters, uint index)
		{
			this->pool = pool;
			if (filters.size() > 0)
			{
				root = new treenode(false);
				unordered_map<int, int> count;
				for (tarray<int>* _array : filters)
				{
					for (int i = 0, size = _array->size(); i < size; i++)
					{
						int value = _array->at(i);
						if (value >= 0 && value != (int) index)
						{
							if (count.find(value) != count.end())
							{
								count[value] = count[value] + 1;
							}
							else
							{
								count[value] = 1;
							}
						}
					}
				}
				// 公告元素
				for (auto& entry : count)
				{
					if (entry.second == (int) filters.size() && shardSize < 10)
					{
						shardIndex[shardSize++] = entry.first;
					}
				}
				int notfilter[16];
				for (auto& vint : filters)
				{
					int addsize = cp2buffer(vint, buffer, notfilter, index, shardSize == 0);
					if (addsize > 0)
					{
						this->add(root, buffer, addsize);
					}
				}
			}
		}
	};
	class datanode
	{
	public:
		int64 key { 0 };
		int64 sort { 0 };
		//offset
		uint v1 { 0 };
		//size
		int v2 { 0 };

		datanode() = default;
		~datanode() = default;

		datanode(int64 key, int64 sort)
		{
			this->set(key, sort);
		}

		void set(int64 key, int64 sort)
		{
			this->key = key;
			this->sort = sort;
		}

		inline bool contains(uint* p0, treeanalysis* alysis, stopdecoder& decoder)
		{
			return alysis->root->find(p0 + this->v1, this->v2);
		}

		inline bool contains(char* p0, treeanalysis* alysis, stopdecoder& decoder)
		{
			decoder.reset(p0 + v1, v2, false);
			return alysis->root->find(decoder);
		}

		inline int compare(datanode* node)
		{
			return this->sort < node->sort ? -1 : (this->sort > node->sort ? 1 : comparekey(node));
		}

		inline int comparekey(datanode* node)
		{
			return this->key < node->key ? -1 : (this->key > node->key ? 1 : 0);
		}
	};

	template<class c> class datapage
	{
	private:
		c* mems;
		datanode* nodes;
		element2<uint, uint> nodesps;
		element3<uint, uint, uint> property;
	public:
		datapage(int mem_capacity, int node_capacity)
		{
			this->property.v2 = mem_capacity;
			this->nodesps.v2 = node_capacity;
			this->mems = new c[mem_capacity];
			this->nodes = new datanode[node_capacity];
		}
		~datapage()
		{
			delete[] this->mems;
			delete[] this->nodes;
		}
		inline c* get(uint id, int& size)
		{
			datanode* node = nodes + xpos(id);
			size = node->v2;
			if (size > 0)
			{
				return this->mems + node->v1;
			}
			return null;
		}

		inline uint xpos(uint id)
		{
			return id % this->nodesps.v2;
		}

		inline datanode* get(uint id)
		{
			return nodes + xpos(id);
		}

		inline datanode* get(uint id, uint* p1, ushort p1len)
		{
			datanode* node = get(id);
			if (node->contains(this->mems, p1, p1len))
			{
				return node;
			}
			else
			{
				return null;
			}
		}

		inline bool contains(datanode* node, treeanalysis* alysis, stopdecoder& decoder)
		{
			return alysis->size() == 0 || node->contains(this->mems, alysis, decoder);
		}

		bool remove(uint id)
		{
			uint index = xpos(id);
			if (this->nodes[index].v2 > 0)
			{
				this->property.v3 += this->nodes[index].v2;
				this->nodes[index].v2 = -this->nodes[index].v2;
				this->nodesps.v1--;
				return true;
			}
			else
			{
				return false;
			}
		}

		int insert(uint id, int64 key, int64 sort, c* p, int len)
		{
			uint pos = xpos(id);
			this->nodes[pos].set(key, sort);
			c* temp = datacp(pos, mems, p, len, nodes, nodesps, property, true);
			if (temp != null)
			{
				this->mems = temp;
				return 1;
			}
			else
			{
				return 0;
			}
		}
	};

	template<class c> class memstore
	{
		datapage<c>** pages;
		uint length;
		uchar pagebit;
		uint pagelength;
	private:
		inline uint index(uint id)
		{
			return id >> pagebit;
		}
	public:
		memstore(int pagelength, uint pagebit)
		{
			this->length = 1;
			this->pagebit = pagebit;
			this->pagelength = pagelength;
			this->pages = new datapage<c>*[length];
			this->pages[0] = new datapage<c>(this->pagelength, 1 << pagebit);
		}
		~memstore()
		{
			if (this->pages != null)
			{
				for (uint i = 0; i < length; i++)
				{
					delete this->pages[i];
				}
				delete[] this->pages;
			}
		}

		inline datapage<c>* pageno(uint id)
		{
			return this->pages[index(id)];
		}

		inline c* get(uint id, int& size)
		{
			return this->pages[index(id)]->get(id, size);
		}
		inline datanode* get(uint id)
		{
			return this->pages[index(id)]->get(id);
		}

		void insert(uint id, int64 key, int64 sort, c* p, int len)
		{
			ensureCapacity(id);
			this->pages[index(id)]->insert(id, key, sort, p, len);
		}

		void ensureCapacity(uint id)
		{
			if (index(id) >= this->length)
			{
				datapage<c>** temp = new datapage<c>*[this->length + 1];
				memcpy(temp, this->pages, sizeof(datapage<c>*) * this->length);
				temp[this->length] = new datapage<c>(this->pagelength, 1 << pagebit);
				delete[] this->pages;
				this->pages = temp;
				this->length += 1;
			}
		}
		void remove(uint id)
		{
			this->pages[index(id)]->remove(id);
		}
	};
}
#endif
