/*
 * sequence.h
 *  序列号，用于分发和回收序列号
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef  SEQUENCE_H_
#define  SEQUENCE_H_
#pragma once
#include "elastici2i.h"
namespace qstardb
{
	class sequence
	{
	private:
		int seq { 0 };
		uint* deleteids;
		int deletesize { 0 };
		int arraylength { 1024 * 16 };
		int extcapacity { 1024 * 64 };
	public:
		sequence()
		{
			this->deleteids = new uint[this->arraylength];
		}
		~sequence()
		{
			delete[] deleteids;
		}

		void recycle(uint id)
		{
			if (deletesize == arraylength)
			{
				arraylength += extcapacity;
				uint* temp = new uint[arraylength];
				memmove(temp, deleteids, sizeof(uint) * deletesize);
				delete[] this->deleteids;
				this->deleteids = temp;
			}
			deleteids[deletesize++] = id;
		}
		int get()
		{
			return this->seq;
		}

		void set(int value)
		{
			this->seq = value;
		}

		int createsq()
		{
			if (deletesize > 0)
			{
				return deleteids[--deletesize];
			}
			else
			{
				return seq++;
			}
		}
	};
	/* 从 int64 主键转化为一个顺序增长的int32序列号*/
	class seq64to32
	{

	private:
		sequence* seq;
		rwsyslock rwlock;
		elasticsmap::emapi2i<qstardb::int64, uint>* table;
	public:
		seq64to32()
		{
			this->seq = new sequence();
			this->table = new elasticsmap::emapi2i<int64, uint>(1024);
		}

		~seq64to32()
		{
			delete this->seq;
			delete this->table;
		}
		bool create(int64 key, uint& value)
		{
			bool result = false;
			rwlock.wrlock();
			if (!this->table->get(key, value))
			{
				int temp = this->seq->createsq();
				this->table->add(key, temp);
				value = temp;
				result = true;
			}
			rwlock.unwrlock();
			return result;
		}

		bool exists(int64 key)
		{

			uint innerkey;
			rwlock.rdlock();
			bool result = table->get(key, innerkey);
			rwlock.unrdlock();
			return result;
		}

		bool exists(int64 key, uint& innerkey)
		{
			rwlock.rdlock();
			bool result = table->get(key, innerkey);
			rwlock.unrdlock();
			return result;
		}

		bool remove(int64 key, uint& rmseq)
		{
			rwlock.wrlock();
			if (table->get(key, rmseq))
			{
				this->seq->recycle(rmseq);
				table->remove(key);
				rwlock.unwrlock();
				return true;
			}
			else
			{
				rwlock.unwrlock();
				return false;
			}
		}
	};
}
#endif
