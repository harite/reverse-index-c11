/*
 * dictionary.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef  DICTIONARY_H_
#define DICTIONARY_H_
#include "rwsyslock.h"
#include "sequence.h"
namespace qstardb
{
// 基于序列号存储 char*
	class strblock
	{
	private:
		char* mems;
		twouint nodesps;
		threeiii properties;
		element2<uint, char>* nodes;
		inline uint xpos(uint seq)
		{
			return seq % nodesps.v2;
		}
	public:
		strblock(int nodelen)
		{
			this->nodesps.v2 = nodelen;
			this->properties.v2 = nodelen * 20;
			this->mems = new char[this->properties.v2];
			this->nodes = new element2<uint, char> [this->nodesps.v2];
		}
		~strblock()
		{
			delete[] this->nodes;
			delete[] this->mems;
		}

		const bool get(uint seq, string& word)
		{
			uint index = xpos(seq);
			if (nodes[index].v2 > 0)
			{
				word.append(mems + nodes[index].v1, nodes[index].v2);
				return true;
			}
			else
			{
				return false;
			}
		}
		inline int compare(uint seq, const char* ch1, uint len1)
		{
			uint index = xpos(seq);
			uint len0 = nodes[index].v2;
			char* ch0 = this->mems + nodes[index].v1;
			if (len0 == len1)
			{
				for (uint i = 0; i < len1; i++)
				{
					if (ch0[i] > ch1[i])
					{
						return 1;
					}
					else if (ch0[i] < ch1[i])
					{
						return -1;
					}
				}
				return 0;
			}
			else
			{
				return len0 - len1;
			}
		}
		bool remove(uint seq)
		{
			uint pos = xpos(seq);
			if (nodes[pos].v2 > 0)
			{
				this->properties.v3 += nodes[pos].v2;
				nodes[pos].v2 *= -1;
				this->nodesps.v1--;
				return true;
			}
			else
			{
				return false;
			}
		}
		int insert(uint seq, const char* p, char len)
		{
			uint index = xpos(seq);
			char* temp = datacp(index, this->mems, p, len, this->nodes, this->nodesps, this->properties, true);
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
// 通过顺序序列号存储字符串
	class strsegs
	{
	private:
		uint size { 0 };
		uint pagesize;
		strblock** pages { null };
		inline uint index(uint seq)
		{
			return seq / pagesize;
		}
		void ensureCapacity(uint seq)
		{
			uint newlen = index(seq);
			if (newlen >= size)
			{
				strblock** temp = new strblock*[newlen + 1];
				for (uint i = this->size; i < newlen + 1; i++)
				{
					temp[i] = new strblock(this->pagesize);
				}
				if (this->pages != null)
				{
					memcpy(temp, this->pages, sizeof(strblock*) * this->size);
					delete[] this->pages;
				}
				this->pages = temp;
				this->size = newlen + 1;
			}
		}
	public:
		strsegs(int pagesize)
		{
			this->pagesize = pagesize;
		}
		~strsegs()
		{
			if (this->pages != null)
			{
				for (uint i = 0; i < size; i++)
				{
					delete this->pages[i];
				}
				delete[] this->pages;
			}
		}
		inline bool get(uint seq, string& word)
		{
			uint pos = index(seq);
			if (pos >= this->size)
			{
				return false;
			}
			else
			{
				return this->pages[pos]->get(seq, word);
			}
		}
		inline int compare(uint seq, const string& term)
		{
			return this->pages[index(seq)]->compare(seq, term.c_str(), term.size());
		}

		inline int compare(uint seq, const char* str, int length)
		{
			return this->pages[index(seq)]->compare(seq, str, length);
		}

		inline void insert(int seq, const char* ch, char len)
		{
			ensureCapacity(seq);
			pages[index(seq)]->insert(seq, ch, len);
		}
		inline void remove(uint seq)
		{
			this->pages[index(seq)]->remove(seq);
		}
	};
//存储字符串对于的整形序列号
	class seqpage
	{
	private:
		uint size { 0 };
		uint length;
		uint* seqs;
		strsegs* segs;
		sequence* seq;
		inline int compare(const char* ch0, int len0, const char* ch1, int len1)
		{
			if (len0 == len1)
			{
				for (int i = 0; i < len1; i++)
				{
					if (ch0[i] > ch1[i])
					{
						return 1;
					}
					else if (ch0[i] < ch1[i])
					{
						return -1;
					}
				}
				return 0;
			}
			else
			{
				return len0 - len1;
			}
		}
	public:
		inline int compare(uint seq, const string& term)
		{
			return this->segs->compare(seq, term);
		}
		inline int compare(uint seq, const char* str, int length)
		{
			return this->segs->compare(seq, str, length);
		}

		int indexof(uint* keys, const char* key, int length, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = compare(keys[mid], key, length);
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
					return fromIndex > size ? size : fromIndex;
				case type_ceil:
					return fromIndex;
				case type_index:
					return -(fromIndex + 1);
				default:
					return toIndex;
			}
		}

		seqpage(sequence* seq, strsegs* segs, int node_len)
		{
			this->seq = seq;
			this->segs = segs;
			this->length = node_len;
			this->seqs = new uint[node_len];
		}

		~seqpage()
		{
			delete[] this->seqs;
		}

		inline int get(const char* str, int length)
		{
			int pos = indexof(this->seqs, str, length, this->size, type_index);
			return pos >= 0 ? seqs[pos] : MIN_INT_VALUE;
		}

		inline int create(const char* str, int length, int value)
		{
			int pos = indexof(this->seqs, str, length, this->size, type_insert);
			if (pos >= 0)
			{
				ensuresize(1);
				int movenum = this->size - pos;
				if (movenum > 0)
				{
					memmove(seqs + pos + 1, seqs + pos, sizeof(uint) * movenum);
				}
				seqs[pos] = value < 0 ? seq->createsq() : value;
				if (seq->get() <= value)
				{
					seq->set(value + 1);
				}
				segs->insert(seqs[pos], str, length);
				this->size++;
				return seqs[pos];
			}
			else
			{
				return seqs[-pos - 1];
			}
		}

		void ensuresize(int len)
		{
			if (this->size + len >= this->length)
			{
				this->length += 256 + size + len;
				uint* temp = new uint[this->length];
				memcpy(temp, this->seqs, sizeof(uint) * this->size);
				delete[] this->seqs;
				this->seqs = temp;
			}
		}

		inline int remove(const char* str, int length)
		{
			int pos = indexof(this->seqs, str, length, this->size, type_index);
			if (pos >= 0)
			{
				int value = seqs[pos];
				segs->remove(value);
				int movenum = this->size - pos - 1;
				if (movenum > 0)
				{
					memmove(seqs + pos, seqs + pos + 1, sizeof(uint) * movenum);
				}
				this->size--;
				this->seq->recycle(value);
				return value;
			}
			else
			{
				return -1;
			}
		}
	};

	class dictable
	{
		uint mode;
		// seq-block
		seqpage** pages;
		// seq-to-str
		strsegs* segs;
		sequence* seq;
		rwsyslock rwlock;
	private:
		inline uint strhash(const char* str, int size)
		{
			uint hash = 0;
			while (size-- > 0)
			{
				hash = hash * 31 + (*str++);
			}
			return hash;
		}
	public:
		dictable(uint pagebit)
		{

			this->seq = new sequence();
			this->segs = new strsegs(1024 * 32);
			if (pagebit > 16 || pagebit <= 10)
			{
				this->mode = (1 << 12) - 1;
			}
			else
			{
				this->mode = (1 << pagebit) - 1;
			}
			this->pages = new seqpage*[this->mode + 1];
			for (uint i = 0; i <= this->mode; i++)
			{
				this->pages[i] = new seqpage(this->seq, this->segs, 128);
			}
		}
		~dictable()
		{
			delete this->seq;
			delete this->segs;
			for (uint i = 0; i <= this->mode; i++)
			{
				delete this->pages[i];
			}
			delete[] this->pages;
		}
		//获取当前分配的最大序列号
		int get()
		{
			return this->seq->get();
		}

		int get(const char* str, int length)
		{
			uint _index = strhash(str, length);
			rwlock.rdlock();
			int result = this->pages[mode & _index]->get(str, length);
			rwlock.unrdlock();
			return result;
		}

		/*通过索引号查找数据*/
		bool get(uint seq, string& word)
		{
			rwlock.rdlock();
			bool result = this->segs->get(seq, word);
			rwlock.unrdlock();
			return result;
		}

		/*插入数据*/
		int insert(const char* str, int length, int value)
		{
			uint _index = strhash(str, length);
			rwlock.wrlock();
			int result = this->pages[mode & _index]->create(str, length, value);
			rwlock.unwrlock();
			return result;
		}
		/*删除数据**/
		int remove(const char* str, int length)
		{
			uint _index = strhash(str, length);
			rwlock.wrlock();
			int result = this->pages[mode & _index]->remove(str, length);
			rwlock.unwrlock();
			return result;
		}
	};

	class dictionary
	{
	private:
		bool ignorecase;
		dictable* word_index;
		static const int maxlen = 127;
	public:
		dictionary(bool ignorecase)
		{
			this->ignorecase = ignorecase;
			this->word_index = new dictable(14);
		}
		~dictionary()
		{
			delete word_index;
		}

		int getCurSeq()
		{
			return this->word_index->get();
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
					int value = word_index->insert(word.c_str(), word.size(), -1);
					indexs.insert(value);
				}
			}
			return indexs;
		}

		/**将词分配指定索引号*/
		inline int add(string& word, int index)
		{
			if (word.length() < maxlen)
			{
				if (ignorecase)
				{
					transform(word.begin(), word.end(), word.begin(), ::toupper);
				}
				return word_index->insert(word.c_str(), word.size(), index);
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
	};
}
#endif
