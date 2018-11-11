/*
 * exeplan.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef EXEPLAN_H_
#define EXEPLAN_H_
#include <unordered_map>
#include "datastore.h"
#include "dataindex.h"
namespace qstardb
{
	template<class c> class execution
	{
	private:
		dictionary* dic { null };
		segments<c>* segs { null };
		memstore<c>* store { null };
		treepool* tnodepool { null };
		iditerpool<c>* iterpool { null };
	public:

		execution(segments<c>* segs, memstore<c>* store, dictionary* dic)
		{
			this->dic = dic;
			this->segs = segs;
			this->store = store;
			this->tnodepool = new treepool();
			this->iterpool = new iditerpool<c>(segs, store);
		}

		~execution()
		{
			delete this->tnodepool;
			delete this->iterpool;
		}

		/**获取每一个and组合的最小计算代价*/
		int getMinScanSize(tarray<int>* filter, unordered_map<int, int>& codeScanSize, int& index)
		{
			int scansize = -1;

			for (int i = 0, size = filter->size(); i < size; i++)
			{
				int value = filter->at(i);
				if (value >= 0 && (scansize == -1 || codeScanSize[value] < scansize))
				{
					index = value;
					scansize = codeScanSize[value];
				}
			}
			//全都是非
			if (filter->size() > 0 && scansize == -1)
			{
				filter->addAND(0);
				index = 0;
				scansize = codeScanSize[0];
			}
			return scansize;
		}

		//计算每个代码的扫描数量
		void setIndexSize(vector<tarray<int>*>& indexs, int64 _s_sort_, int64 _e_sort_, unordered_map<int, int>& codeScanSize)
		{
			if (codeScanSize.find(0) == codeScanSize.end())
			{
				codeScanSize[0] = this->segs->scansize(0, _s_sort_, _e_sort_);
			}

			for (const auto& _array : indexs)
			{
				for (int i = 0, size = _array->size(); i < size; i++)
				{
					int index = _array->at(i);
					if (index >= 0 && codeScanSize.find(index) == codeScanSize.end())
					{
						codeScanSize[index] = this->segs->scansize(index, _s_sort_, _e_sort_);
					}
				}
			}
		}

		/**获取所有and组合的计算代价*/
		int bestScan(vector<tarray<int>*>& indexs, unordered_map<int, int>& codeScanSize, unordered_map<int, vector<tarray<int>*>>& result)
		{
			//所有组合的计算代价
			int allSum = 0;
			//记录每个组合的最小计算代价
			unordered_map<int, int> codeUseSize;
			map<tarray<int>*, int> statistics;
			for (const auto& _array : indexs)
			{
				int index = 0;
				int _scansize = getMinScanSize(_array, codeScanSize, index);
				//如果组合扫描数量小于 等于 0，则直接抛弃
				if (_scansize > 0)
				{
					statistics[_array] = _scansize;
					allSum += _scansize;
					for (int i = 0, size = _array->size(); i < size; i++)
					{
						int value = _array->at(i);
						if (value >= 0)
						{
							//获取最优代价比
							if (codeUseSize.find(value) != codeUseSize.end())
							{
								codeUseSize[value] = codeUseSize[value] + _scansize;
							}
							else
							{
								codeUseSize[value] = _scansize;
							}
						}
					}

				}
			}
			return bestScan(statistics, codeScanSize, codeUseSize, result, allSum);
		}

		int bestScan(map<tarray<int>*, int>& statistics, unordered_map<int, int>& codeScanSize, unordered_map<int, int>& codeUseSize,
				unordered_map<int, vector<tarray<int>*>>& result, double allSum)
		{
			int sum = 0;
			while (statistics.size() > 0)
			{
				double maxValue = -1;
				int index = -1;
				for (const auto& entry : codeUseSize)
				{
					// （影响数 / 扫描数）  = 效率
					double scale = codeUseSize[entry.first] * 1.0f / codeScanSize[entry.first];
					//cout << "scale:" << scale << endl;
					if (scale > 1.1)
					{
						// 效率 * 效率 * 占比 = 影响结果值
						double value = scale * (codeUseSize[entry.first] * 1.0f / allSum);
						if (value > maxValue)
						{
							index = entry.first;
							maxValue = value;
						}

					}
				}
				if (index >= 0)
				{
					//存在最优化分拣
					vector<tarray<int>*> vsint;
					result[index] = vsint;
					for (auto& entry : statistics)
					{
						//将所有包含了最优化所有的组合提出，按照最优化数据列进行处理
						for (int i = 0, size = entry.first->size(); i < size; i++)
						{
							if (index == entry.first->at(i))
							{
								result[index].push_back(entry.first);
								for (int j = 0; j < size; j++)
								{
									codeUseSize[entry.first->at(j)] -= entry.second;
								}
								entry.second = 0;
								break;
							}
						}
					}
					if (result[index].size() > 0)
					{
						for (const auto& sint : result[index])
						{
							statistics.erase(sint);
						}
					}
					else
					{
						result.erase(index);
					}
					sum += codeScanSize[index];
					codeUseSize.erase(index);
				}
				else
				{
					//已经不存在最优化数据了，直接处理处理每一个数据
					for (const auto& entry : statistics)
					{
						for (int i = 0, size = entry.first->size(); i < size; i++)
						{
							int value = entry.first->at(i);
							if (value < 0)
							{
								continue;
							}
							if (codeScanSize[value] == entry.second)
							{
								if (result.find(value) == result.end())
								{
									vector<tarray<int>*> vsint;
									result[value] = vsint;
								}
								result[value].push_back(entry.first);
								break;
							}
						}
						sum += entry.second;
					}
					//清空所有数据
					statistics.clear();
				}
			}
			return sum;
		}

		int fastScan(map<tarray<int>*, int>& statistics, unordered_map<int, int>& codeScanSize, unordered_map<int, int>& codeUseSize,
				unordered_map<int, vector<tarray<int>*>>& result, double allSum)
		{
			vector<int> list;
			int sum = 0;
			for (const auto& entry : codeUseSize)
			{
				// （影响数 / 扫描数）  = 效率
				double scale = codeUseSize[entry.first] * 1.0f / codeScanSize[entry.first];
				//cout << "scale:" << scale << endl;
				if (scale > 1.2)
				{
					// 效率 * 效率 * 占比 = 影响结果值
					list.push_back(entry.first);
				}
			}

			while (statistics.size() > 0)
			{
				int index = -1;
				for (int i = list.size() - 1; i >= 0; i--)
				{
					int codeValue = list.at(i);
					// （影响数 / 扫描数）  = 效率
					double scale = codeUseSize[codeValue] * 1.0f / codeScanSize[codeValue];
					//cout << "scale:" << scale << endl;
					if (scale >= 1.2)
					{
						// 效率 * 效率 * 占比 = 影响结果值
						index = codeValue;
						break;
					}
					else
					{
						list.pop_back();
					}
				}
				if (index >= 0)
				{
					//存在最优化分拣
					vector<tarray<int>*> vsint;
					result[index] = vsint;
					for (auto& entry : statistics)
					{
						//将所有包含了最优化所有的组合提出，按照最优化数据列进行处理
						for (int i = 0, size = entry.first->size(); i < size; i++)
						{
							if (index == entry.first->at(i))
							{
								result[index].push_back(entry.first);
								for (int j = 0; j < size; j++)
								{
									codeUseSize[entry.first->at(j)] -= entry.second;
								}
								entry.second = 0;
								break;
							}
						}
					}
					if (result[index].size() > 0)
					{
						for (const auto& sint : result[index])
						{
							statistics.erase(sint);
						}
					}
					else
					{
						result.erase(index);
					}
					sum += codeScanSize[index];
					codeUseSize.erase(index);
				}
				else
				{
					//已经不存在最优化数据了，直接处理处理每一个数据
					for (const auto& entry : statistics)
					{
						for (int i = 0, size = entry.first->size(); i < size; i++)
						{
							int value = entry.first->at(i);
							if (value < 0)
							{
								continue;
							}
							if (codeScanSize[value] == entry.second)
							{
								if (result.find(value) == result.end())
								{
									vector<tarray<int>*> vsint;
									result[value] = vsint;
								}
								result[value].push_back(entry.first);
								break;
							}
						}
						sum += entry.second;
					}
					//清空所有数据
					statistics.clear();
				}
			}
			return sum;
		}

		searchstats& execute(unordered_map<int, vector<tarray<int>*>>& mapindex, vector<tarray<int>*>& filter, int64 _s_sort_, int64 _e_sort_, bool desc,
				searchstats& stat)
		{
			dataqueue < c > queue(this->iterpool, this->tnodepool, mapindex.size(), filter);

			for (auto& entry : mapindex)
			{
				auto list = this->segs->idslist(entry.first, _s_sort_, _e_sort_, desc);
				queue.additer(list, tnodepool, desc, entry.first, entry.second);
			}
			while ((!stat.pagefull() || stat.isexecount()) && queue.hasnext())
			{
				int64 key = queue.next()->key;
				if (!stat.pagefull() && stat.startAddNode())
				{
					stat.add(key);
				}
				stat.next();
			}
			return stat;
		}
	};

}
#endif 
