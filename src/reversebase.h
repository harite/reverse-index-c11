#pragma once
/*
 * qstarindex.h
 *
 *  Created on: 2015��2��10��
 *      Author: jkuang
 */
#ifndef REVERSEBASE_H_
#define REVERSEBASE_H_
#include "reverseindex.h"
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
		rmindex* index;
		cache::kvcache* cache;;
	public:
		rmdb(bool ignoreCase, bool compress)
		{
			this->cache = new cache::kvcache();
			this->index = new rmindex(ignoreCase, compress);
		}
		~rmdb()
		{
			delete index;
			delete cache;
		}

		void add(int64 key, int64 sort, vector<string> &terms,const char* data, int length)
		{
			this->index->add(key, sort, terms);
			this->cache->add(key, data, length);
		}

		void remove(int64 key)
		{
			this->index->remove(key);
			this->cache->remove(key);
		}

		charwriter& query(vector<int64>& _keys,  charwriter& writer)
		{
			searchstats stat(0, 0, 0);
			vector<int64>& keys = this->index->query(_keys, stat).result();
			int length = keys.size();
			writer.writeInt(length);
			for (int i = 0; i < length; i++)
			{
				writer.writeInt64(keys.at(i));
				int datalength = 0;
				const char* data = cache->get(keys.at(i), datalength);
				if (data != nullptr && datalength > 0)
				{
					writer.writeInt(datalength);
					writer.write(data, datalength);
				}
				else
				{
					//�����쳣���Ѿ��Ҳ�������
					writer.writeInt(0);
				}
			}
			return writer;
		}

		charwriter& query(const string& syntax, int64 _s_sort_, int64 _e_sort_, bool desc, searchstats& stat, charwriter& writer)
		{
			vector<int64>& keys = this->index->query(syntax, _s_sort_, _e_sort_, desc, stat).result();
			int length = keys.size();
			writer.writeInt(length);
			for (int i = 0; i < length; i++)
			{
				writer.writeInt64(keys.at(i));
				int datalength = 0;
				const char* data = cache->get(keys.at(i), datalength);
				if (data != nullptr && datalength > 0)
				{
					writer.writeInt(datalength);
					writer.write(data, datalength);
				}
				else
				{
					//�����쳣���Ѿ��Ҳ�������
					writer.writeInt(0);
				}
			}
			return writer;
		}
	};
}
#endif
