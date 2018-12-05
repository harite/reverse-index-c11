#pragma once
/*
 * qstarindex.h
 *
 * 
 *      Author: jkuang
 */
#ifndef REVERSEBASE_H_
#define REVERSEBASE_H_
#include "reverseindex.h"
#include "btreei2b.h"
#include "seqmapi2b.h"
using namespace std;
using namespace qstardb;
namespace reverse {


	class rmindex
	{
	private:
		bool compress;
		stardb<char> *chdb{ null };
		stardb<uint> *uintdb{ null };
	public:
		rmindex(bool ignoreCase, bool compress)
		{
			this->compress = compress;
			if (this->compress)
			{
				chdb = new stardb<char>(ignoreCase);
			}
			else
			{
				uintdb = new stardb<uint>(ignoreCase);
			}
		}
		~rmindex()
		{
			compress ? delete chdb : delete uintdb;
		}
		void add(int64 key, int64 sort, vector<string> &terms)
		{
			compress ? chdb->add(key, sort, terms) : uintdb->add(key, sort, terms);
		}

		void load(string& file)
		{
			compress ? chdb->readfile(file) : uintdb->readfile(file);
		}

		void dump(string& file)
		{
			compress ? chdb->writefile(file) : uintdb->writefile(file);
		}

		bool remove(int64 key)
		{
			return compress ? chdb->remove(key) : uintdb->remove(key);
		}

		searchstats& query(vector<int64>& keys, searchstats& stat)
		{
			return compress ? chdb->query(keys, stat) : uintdb->query(keys, stat);
		}

		searchstats& query(const string& syntax, int64 _s_sort_, int64 _e_sort_, bool desc, searchstats& stat)
		{
			return compress ? chdb->query(syntax, _s_sort_, _e_sort_, desc, stat) : uintdb->query(syntax, _s_sort_, _e_sort_, desc, stat);
		}
	};


	class rmdb
	{
	private:
		rwsyslock lock;
		rmindex* index;
		seqmap::seqcache* cache;;
	public:
		rmdb(bool ignoreCase, bool compress)
		{
			this->cache = new seqmap::seqcache(8);
			this->index = new rmindex(ignoreCase, compress);
		}
		~rmdb()
		{
			delete index;
			delete cache;
		}

		void add(int64 key, int64 sort, vector<string> &terms,const char* data, int length)
		{
			lock.wrlock();
			this->index->add(key, sort, terms);
			this->cache->add(key, data, length);
			lock.unwrlock();
		}

		void remove(int64 key)
		{
			lock.wrlock();
			this->index->remove(key);
			this->cache->remove(key);
			lock.unwrlock();
		}

		charwriter& query(vector<int64>& _keys,  charwriter& writer)
		{
			searchstats stat(0, 0, 0);
			lock.rdlock();
			vector<int64>& keys = this->index->query(_keys, stat).result();
			int length = keys.size();
			writer.writeInt(length);
			for (int i = 0; i < length; i++)
			{
				writer.writeInt64(keys.at(i));
				cache->find(keys.at(i), writer);
			}
			lock.unrdlock();
			return writer;
		}

		charwriter& query(const string& syntax, int64 _s_sort_, int64 _e_sort_, bool desc, searchstats& stat, charwriter& writer)
		{
			lock.rdlock();
			vector<int64>& keys = this->index->query(syntax, _s_sort_, _e_sort_, desc, stat).result();
			int length = keys.size();
			writer.writeInt(length);
			for (int i = 0; i < length; i++)
			{
				writer.writeInt64(keys.at(i));
				cache->find(keys.at(i), writer);
			}
			lock.unrdlock();
			return writer;
		}
	};
}
#endif
