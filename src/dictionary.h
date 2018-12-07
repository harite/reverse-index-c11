/*
 * dictionary.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  DICTIONARY_H_
#define DICTIONARY_H_
#include "rwsyslock.h"
#include "seqmapb2i.h"
namespace qstardb
{
	class dictionary
	{
	private:
		bool ignorecase;
		seqmap::b2imap* word_index;
		static const int maxlen = 127;
	public:
		dictionary(bool ignorecase)
		{
			this->ignorecase = ignorecase;
			this->word_index = new seqmap::b2imap(128);
		}
		~dictionary()
		{
			delete word_index;
		}

		/**批量添加词汇*/
		set_int& add(vector<string> &words, set_int& indexs)
		{

			for (uint i = 0; i < words.size(); i++)
			{
				string& word = words.at(i);
				if (word.length() < maxlen)
				{
					if (ignorecase)
					{
						transform(word.begin(), word.end(), word.begin(), ::toupper);
					}
					int value = word_index->add(word.c_str(), word.size());
					indexs.insert(value);
				}
			}
			return indexs;
		}

		/**将词分配指定索引号*/
		inline int add(string& word)
		{
			if (word.length() < maxlen)
			{
				if (ignorecase)
				{
					transform(word.begin(), word.end(), word.begin(), ::toupper);
				}
				return word_index->add(word.c_str(), word.size());
			}
			else
			{
				return MIN_INT_VALUE;
			}
		}
		/*词典数据，通过词查找分配的索引号，若不存在则返回-1**/
		inline int get(string word)
		{
			if (word.length() < maxlen)
			{
				if (ignorecase)
				{
					transform(word.begin(), word.end(), word.begin(), ::toupper);
				}
				return this->word_index->get(word.c_str(), word.size());
			}
			else
			{
				return MIN_INT_VALUE;
			}
		}

		inline int get(char* str, int length)
		{
			if (length < maxlen)
			{
				if (ignorecase)
				{
					for (int i = 0; i < length; i++)
					{
						char ch = str[i];
						str[i] = (ch >= 'a' && ch <= 'z') ? (ch & 0xdf) : ch;
					}
				}
				return this->word_index->get(str, length);
			}
			else
			{
				return MIN_INT_VALUE;
			}
		}
		//根据分配的索引号提取词条
		inline bool get(int seq, string& word)
		{
			return this->word_index->get(seq, word);
		}

		inline int curSeq() {
			return this->word_index->curSeq();
		}
	};
}
#endif
