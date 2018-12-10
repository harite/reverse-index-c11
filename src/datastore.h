/*
 * datastore.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  DATASTORE_H_
#define  DATASTORE_H_
#pragma once
#include "operateand.h"
using namespace std;
namespace qstardb
{

	
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
			return nullptr;
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
				return  nullptr;
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
			if (temp != nullptr)
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
			if (this->pages != nullptr)
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
