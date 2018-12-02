/*
 * searchstats.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  SEARCHSTATS_H_
#define  SEARCHSTATS_H_
#include "dxtemplate.h"
namespace qstardb
{
	class searchstats
	{
	private:
		uint start;

		uint rows;

		uint count;

		uint offset { 0 };

		vector<int64> _result;

	public:
		searchstats(uint start, uint rows, uint count)
		{
			this->start = start;
			this->rows = rows;
			this->count = count;
		}

		~searchstats() = default;

		vector<int64>& result()
		{
			return this->_result;
		}

		int64 getoffset()
		{
			return this->offset;
		}

		void next()
		{
			++this->offset;
		}

		void addoffset(int offset)
		{
			this->offset += offset;
		}

		void setoffset(int offset)
		{
			this->offset = offset;
		}

		inline bool pagefull()
		{
			return _result.size() >= this->rows;
		}

		void add(int64 key)
		{
			_result.push_back(key);
		}
		inline bool startAddNode()
		{
			return this->offset >= this->start;
		}

		inline bool isexecount()
		{
			return this->count > offset;
		}

		uint getrows()
		{
			return this->rows;
		}

		uint getstart()
		{
			return start;
		}
	};
}
#endif
