/*
 * qstarindex.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef QSTARINDEX_H_
#define QSTARINDEX_H_
#include "searchstats.h"
#include "syntaxparser.h"
#include "dataqueue.h"
#include "filestream.h"
#include "stopencoder.h"
#include "exeplan.h"
#include  "dicmapi2s.h"
#include <mutex>    

using namespace std;
namespace qstardb
{
	template<class c> class stardb
	{
	private:
		c instance;
		seq64to32 seq;
		mutex mtx; // @suppress("Type cannot be resolved")
		bool ignoreCase;
		memstore<c>* store;
		segments<c>* segs;
		uint* insertbuffer;
		uint* deletebuffer;
		rwsyslock rwlock;
		paramspool* pmpool;
		execution<c>* exeplain;
		/**/
		bool insert(uint key, uint* p, short len)
		{
			for (int i = 0; i < len; i++)
			{
				this->segs->insert(p[i], key);
			}
			return true;
		}

		bool remove(uint key, set_int& indexs)
		{
			for (uint index : indexs)
			{
				this->segs->remove(key, index);
			}
			return true;
		}

		void removedata(int intkey)
		{
			int size = -1;
			c* ptr = store->get(intkey, size);
			if (ptr != null)
			{
				int len = readProperties(ptr, size, deletebuffer);
				for (size_t i = 0; i < len; i++)
				{
					segs->remove(intkey, deletebuffer[i]);
				}
				store->remove(intkey);
			}

		}

		int readProperties(uint* chs, int length, uint* buffer)
		{
			if (chs != null)
			{
				for (int i = 0; i < length; i++)
				{
					buffer[i] = chs[i];
				}
			}
			return length;
		}

		int readProperties(char* chs, int length, uint* buffer)
		{
			int size = 0;
			if (chs != null)
			{
				stopdecoder decoder(chs, length, false);
				while (decoder.hasNext())
				{
					buffer[size++] = decoder.nextSum();
				}
			}
			return size;
		}

		void add(char instance, int64 key, int64 sort, set_int& newIndex)
		{
			int lastValue = 0;
			int indexMark = 0;
			encoder->reset();
			for (int index : newIndex)
			{
				insertbuffer[indexMark++] = index;
				encoder->writeUInt64(index - lastValue);
				lastValue = index;
			}
			this->add(key, sort, insertbuffer, newIndex.size(), encoder->getbuffer(), encoder->getoffset());
		}

		void add(uint instance, int64 key, int64 sort, set_int& newIndex)
		{
			int indexMark = 0;
			for (int index : newIndex)
			{
				insertbuffer[indexMark++] = index;
			}
			this->add(key, sort, insertbuffer, newIndex.size(), insertbuffer, newIndex.size());
		}

		void add(char instance, int64 key, int64 sort, uint* p, int len)
		{
			encoder->reset();
			for (int i = 0; i < len; i++)
			{
				if (i == 0)
				{
					encoder->writeUInt64(p[i]);
				}
				else
				{
					encoder->writeUInt64(p[i] - p[i - 1]);
				}
			}
			this->add(key, sort, p, len, encoder->getbuffer(), encoder->getoffset());
		}

		void add(uint instance, int64 key, int64 sort, uint* p, int len)
		{
			this->add(key, sort, p, len, p, len);
		}
		void add(int64 key, int64 sort, uint* p, int len)
		{
			this->add(instance, key, sort, p, len);
		}

		void add(int64 key, int64 sort, uint* p, int len, c* properties, int plen)
		{
			uint intkey;
			if (!seq.create(key, intkey))
			{
				removedata(intkey);
			}
			store->insert(intkey, key, sort, properties, plen);
			insert(intkey, p, len);
		}

		int addword(string& word, int index)
		{
			return this->dic->add(word, index);
		}
		/**
		 *
		 *
		 * @param arr
		 */
		inline uint* bubbleSort(uint* arr, int length)
		{
			for (int i = 0; i < length - 1; i++)
			{
				bool flag = true;
				for (int j = 0; j < length - 1 - i; j++)
				{
					if (arr[j] > arr[j + 1])
					{
						int v = arr[j];
						arr[j] = arr[j + 1];
						arr[j + 1] = v;
						flag = false;
					}
				}
				if (flag)
				{
					break;
				}
			}
			return arr;
		}

		int write(char* chs, int& size, int* temp)
		{
			stopdecoder decoder(chs, size, false);
			int index = 0;
			while (decoder.hasNext())
			{
				temp[index++] = decoder.nextSum();
			}
			size = index;
			return index;
		}

		int write(uint* chs, int& size, int* temp)
		{
			for (size_t i = 0; i < size; i++)
			{
				temp[i] = chs[i];
			}
			return size;
		}
	
		int64 currentTimeMillis()
		{
			auto time_now = chrono::system_clock::now();
			auto duration_in_ms = chrono::duration_cast<chrono::milliseconds>(time_now.time_since_epoch());
			return duration_in_ms.count();
		}

	public:
		dictionary* dic;
		stopencoder* encoder;
		stardb(bool ignoreCase)
		{
			this->ignoreCase = ignoreCase;
			this->pmpool = new paramspool();
			this->insertbuffer = new uint[1024 * 32];
			this->deletebuffer = new uint[1024 * 32];
			this->encoder = new stopencoder(1024 * 32 * 5);
			this->dic = new dictionary(ignoreCase);
			this->store = new memstore<c>(1024 * 512, 14);
			this->segs = new segments<c>(this->store);
			this->exeplain = new execution<c>(this->segs, this->store, this->dic);
			vector<string> global;
			global.push_back(_all);
			set_int temp;
			this->dic->add(global, temp);
		}

		~stardb()
		{
			delete this->segs;
			delete this->dic;
			delete this->store;
			delete this->encoder;
			delete this->exeplain;
			delete this->pmpool;
			delete[] this->insertbuffer;
			delete[] this->deletebuffer;
		}

		/**/
		void add(int64 key, int64 sort, vector<string> &terms)
		{

			set_int newIndex;
			terms.push_back(_all);
			if (terms.size() > 1024 * 32)
			{
				return;
			}
			rwlock.wrlock();
			dic->add(terms, newIndex);
			this->add(instance, key, sort, newIndex);
			rwlock.unwrlock();
		}

		/**/
		bool remove(int64 key)
		{
			uint intkey;
			rwlock.wrlock();
			if (seq.remove(key, intkey))
			{
				removedata(intkey);
				rwlock.unwrlock();
				return true;
			}
			else
			{
				rwlock.unwrlock();
				return false;
			}
		}
		/*提供id直接查询，验证某条数据是否存在索引中*/
		searchstats& query(vector<int64>& keys, searchstats& stat)
		{
			for (size_t i = 0; i < keys.size(); i++)
			{
				if (this->seq.exists(keys.at(i)))
				{
					stat.add(keys.at(i));
					stat.next();
				}
			}
			return stat;
		}

		/*基于逻辑表达式查询
		 syntax 为需要需要查询的语法
		 _s_sort_ 起始范围值
		 _s_sort_ 截止范围值
		 desc 是否按照降序的方式在指定范围内查找
		 */
		searchstats& query(const string& syntax, int64 _s_sort_, int64 _e_sort_, bool desc, searchstats& stat)
		{
			if (syntax.size() < 0 || !this->segs->hasSegment())
			{
				return stat;
			}
			int parserindex;
			//进行语法解析
			syntaxparser parser(this->dic, this->pmpool);
			//如果语法解析错误，parserindex为遇到错误的位置
			if (!parser.parser(syntax, parserindex))
			{
				//cout << " parser error at " << parserindex << endl;
				return stat;
			}
			vector<tarray<int>*>& indexs = parser.getResult();
			vector<tarray<int>*>& filters = parser.getFilter();
			unordered_map<int, int> codeScanSize; // @suppress("Symbol is not resolved") // @suppress("Type cannot be resolved")
			unordered_map<int, vector<tarray<int>*>> iterator0, iterator1; // @suppress("Symbol is not resolved") // @suppress("Type cannot be resolved")

			//获取各个词条下，文档的数量
			rwlock.rdlock();
			this->exeplain->setIndexSize(indexs, _s_sort_, _e_sort_, codeScanSize);
			this->exeplain->setIndexSize(filters, _s_sort_, _e_sort_, codeScanSize);
			rwlock.unrdlock();

			//计算最优解，生成执行计划的数据结构
			int scanSize0 = this->exeplain->bestScan(indexs, codeScanSize, iterator0);
			int scanSize1 = this->exeplain->bestScan(filters, codeScanSize, iterator1);
			//如果执行计划显示统计数据量为 0,则直接返回
			if (scanSize0 <= 0 && scanSize1 <= 0)
			{
				return stat;
			}
			//	cout << iterator0.size() << " -------" << iterator1.size() << endl;
			//return stat;
			rwlock.rdlock();
			//如果存在filter，且条件统计两大于filter，则扫描filter，条件作为过滤，反之交换filter和条件
			if (filters.size() > 0 && scanSize0 > scanSize1)
			{
				this->exeplain->execute(iterator1, indexs, _s_sort_, _e_sort_, desc, stat);
			}
			else
			{
				this->exeplain->execute(iterator0, filters, _s_sort_, _e_sort_, desc, stat);
			}
			rwlock.unrdlock();
			return stat;
		}

		bool readfile(string& filename)
		{
			if (checkfile(filename))
			{
				filereader reader(filename);
				if (reader.isOpen())
				{
					bool result=read(reader);
					reader.close();
					return result;
				}
				else
				{
					reader.close();
					return false;
				}
			}
			else {
				return false;
			}
	
		}


		bool checkfile(string& filename)
		{
			filereader reader(filename);
		    //int64 HEAD_MARK+ int32 dicsize
			if (reader.hasmore(20)&& reader.readInt64()==HEAD_MARK)
			{
				int64 time = reader.readInt64();
				//dump 已经超过了两天时间
				if (this->currentTimeMillis() - time > 86400l * 1000l*2)
				{
					reader.close();
					return false;
				}
				int dicSize = reader.readInt32();
				// check dic
				char* tempData = new char[32 * 1024 * 4];
				for (int i = 0; i < dicSize; i++)
				{
					if (reader.hasmore(1))
					{
						char len = reader.readChar();
						if (reader.hasmore(len))
						{
							reader.read(tempData,len);
						}
						else {
							reader.close();
							delete[] tempData;
							return false;
						}
					}
					else {
						reader.close();
						delete[] tempData;
						return false;
					}
				}
				//check index
				
				while (reader.hasmore(9)) 
				{
					char mark = reader.readChar();
					int64 key = reader.readInt64();
					if (mark == NODE_ADD) {
						if (reader.hasmore(10))
						{
							int64 sort = reader.readInt64();
							short length = reader.readShort();
							if (reader.hasmore(length*4))
							{
								reader.read(tempData,length*4);
							}
							else
							{
								reader.close();
								delete[] tempData;
								return true;
							}
						}
						else
						{
							reader.close();
							delete[] tempData;
							return true;
						}
					}
					else if (mark==FILE_EOF &&  key==TAIL_MARK)
					{
						reader.close();
						delete[] tempData;
						return true;
						
					}
					else
					{
						reader.close();
						delete[] tempData;
						return false;
					}
				}
				reader.close();
				delete[] tempData;
				return false;
			}
			else
			{
				reader.close();
				return true;
			}
		}

		bool read(filereader& reader)
		{
			if (reader.hasmore(20) && reader.readInt64() == HEAD_MARK)
			{
				int64 time = reader.readInt64();
				if ((this->currentTimeMillis() - time) > 86400l * 1000 * 2l)
				{
					return false;
				}
				int size = reader.readInt32();
				int* dicindexs = new int[size];
				mapi2s::dici2s dmap((size / 256) + 16);
				char temp[128];
				for (int i = 0; i < size; i++)
				{
					char len = reader.readChar();
					reader.read(temp, len);
					dmap.insert(i, (signed char*)temp, len);
					dicindexs[i] = -1;
				}
				uint* tempindexs = new uint[1024 * 32];
				long long sum = 0;
				long long sum1 = 0;
				int count = 0;
				while (reader.hasmore(9))
				{
					if (count++ % 100000 == 99999)
					{
						cout << "add nodes:" << count << " avg:" << sum1 / 100000 << " allAvg:" << sum / count << endl;
						sum1 = 0;
					}
					char mark = reader.readChar();
					int64 key = reader.readInt64();

					if (mark == NODE_ADD)
					{
					
						int64 sort = reader.readInt64();
						short length = reader.readShort();
						sum += length;
						sum1 += length;
						for (int j = 0; j < length; j++)
						{
							int pos = reader.readInt32();
							if (dicindexs[pos] == -1)
							{
								string word;
								if (dmap.get(pos, word))
								{
									dicindexs[pos] = this->dic->add(word);
								}
								else
								{
									cout << "get word error!" << endl;
								}
							}
							tempindexs[j] = dicindexs[pos];

						}
						this->add(key, sort, bubbleSort(tempindexs, length), length);
					}
					else if (mark == FILE_EOF && key==TAIL_MARK) {
						delete[] dicindexs;
						delete[] tempindexs;
						return true;
					}
					else
					{
						delete[] dicindexs;
						delete[] tempindexs;
						return false;
						break;
					}
				}
				delete[] dicindexs;
				delete[] tempindexs;
				return false;
			}
			else {
				return false;
			}
			
		}
		void writefile(string& filename)
		{
			if (!this->segs->hasSegment())
			{
				return;
			}
			string temp = filename;
			temp.append(".temp");
			filewriter fileout(temp);
			//写入词典部分
			int dicSize = this->dic->curSeq();
			fileout.writeInt64(HEAD_MARK);
			fileout.writeInt64(this->currentTimeMillis());
			fileout.writeInt32(dicSize);
			for (int i = 0; i < dicSize; i++)
			{
				string word;
				if (this->dic->get(i, word))
				{
					fileout.writeChar(word.length());
					fileout.writeBytes(word.c_str(), word.length());
				}
				else
				{
					fileout.writeChar(0);
				}
			}
			//获取全局排序索引,在进行dump的时候，索引可读但不允许写
			rwlock.rdlock();
			uint innerkey = 0;
			uint index = this->dic->get(_all);
			datanode s_temp(MIN_VALUE, MIN_VALUE);
			datanode e_temp(MAX_VALUE, MAX_VALUE);
			baseiterator<uint>* iterator = segs->keys(index, &s_temp, &e_temp, false);
			int* buffer = new int[1024 * 32];
			//按照全局排序顺序，依次将数据写入硬盘
			bool error = false;
			while (iterator->hasnext())
			{
				uint id = iterator->next();
				datanode* node = this->store->get(id);
				if (this->seq.exists(node->key, innerkey))
				{
					int size = 0;
					c* properties = this->store->get(innerkey, size);
					if (size > 0)
					{
						fileout.writeChar(NODE_ADD);
						fileout.writeInt64(node->key);
						fileout.writeInt64(node->sort);
						int length = write(properties, size, buffer);
						fileout.writeShort(length);
						for (int i = 0; i < length; i++)
						{
							fileout.writeInt32(buffer[i]);
						}
					}
					else
					{
						cout << "error key:" << node->key << endl;
						fileout.writeChar(DUMP_ERR);
						error = true;
						break;
					}
				}
				else
				{
					cout << "error key:" << node->key << endl;
					fileout.writeChar(DUMP_ERR);
					error = true;
					break;
				}
			}
			//释放读锁
			rwlock.unrdlock();
			iterator->free();
			delete iterator;
			delete[] buffer;
			//标记文件已经写完
			fileout.writeChar(FILE_EOF);
			//给文件盖上结尾标记
			fileout.writeInt64(TAIL_MARK);
			fileout.flush();
			fileout.close();
			//临时文件写完以后，将临时文件重名为正式名称
			if (!error)
			{
				fileout.reNameTo(filename);
				cout << " end of dump ,file name:"<< filename << endl;
			}
		}
	};
}
#endif
