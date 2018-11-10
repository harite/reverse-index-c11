/*
 * syntaxparser.h
 *
 *  Created on: 2015年2月10日
 *      Author: jkuang
 */
#ifndef SYNTAXPARSER_H_
#define SYNTAXPARSER_H_
#include <mutex>  
#include "dxtemplate.h"
#include "dictionary.h"
namespace qstardb
{
	static const char OR = '|';
	static const char AND = '&';
	static const char BLANK = ' ';
	static const char SYNTAX_OK = 1;
	static const char SYNTAX_NOT = 0;
	static const char SYNTAX_ERROR = -1;
	static const char left_bracket = '(';
	static const char right_bracket = ')';
	static const string _all = "_all:*";

// 括号不匹配
	const static char error_bracket = -1;
// 操作数之间缺少操作符
	const static char error_operator = -2;
// 操作符之间缺少操作数
	const static char error_term_less = -3;
// 操作数长度超过上限
	const static char error_term_tolong = -4;

	const static int OP_OR = MAX_INT_VALUE - 1;
	const static int OP_AND = MAX_INT_VALUE - 2;
	const static int LEFT = MAX_INT_VALUE - 3;
	const static int RIGHT = MAX_INT_VALUE - 4;

	template<class t> class tarray
	{
	private:
		int _size { 0 };
		t _array[MAX_AND_SIZE];

	public:
		tarray() = default;
		~tarray() = default;
		void addNew(t value)
		{
			this->_array[this->_size++ % MAX_AND_SIZE] = value;
		}
		void set(t value)
		{
			if (this->_size == 0)
			{
				this->_array[this->_size++ % MAX_AND_SIZE] = value;
			}
		}
		void addAND(int value)
		{
			if (this->_size == 0)
			{
				return;
			}
			else
			{
				this->addNew(value);
			}
		}

		t at(int index)
		{
			return this->_array[index % MAX_AND_SIZE];
		}
		int size()
		{
			return this->_size % MAX_AND_SIZE;
		}
		t* array()
		{
			return _array;
		}
		void reset()
		{
			this->_size = 0;
		}
		//对数组进行排序消重，元素个数从小到大排序
		inline void sortAndDistinct()
		{
			int length = this->size();
			for (int i = 0; i < length - 1; i++)
			{
				bool flag = true;
				for (int j = 0; j < length - 1 - i; j++)
				{
					if (_array[j] > _array[j + 1])
					{
						t v = _array[j];
						_array[j] = _array[j + 1];
						_array[j + 1] = v;
						flag = false;
					}
				}
				if (flag)
				{
					break;
				}
			}
			int temp = length - 1;
			for (int i = 1, j = 0; i < length; i++)
			{
				t tempvalue = _array[j];
				if (tempvalue < 0)
				{
					while (_array[temp] > 0)
					{
						if (tempvalue == -_array[temp--])
						{
							//存在 A and -A 的情况
							this->_size = 0;
							return;
						}
					}
				}
				if (_array[j] == _array[i])
				{
					this->_size--;
					continue;
				}
				else
				{
					_array[++j] = _array[i];
				}
			}
		}
	};

	class valuespool
	{
		int extCapacity { 1024 };
		jstack<tarray<int>*>* back;
		jstack<tarray<int>*>* pool;
		std::mutex lock; // @suppress("Type cannot be resolved")
	public:
		valuespool()
		{
			this->extCapacity = 1024 * 16;
			this->back = new jstack<tarray<int>*>(10);
			this->pool = new jstack<tarray<int>*>(1024 * 128);
			this->createArray(extCapacity);
		}
		~valuespool()
		{
			delete pool;
			while (back->jsize() > 0)
			{
				delete[] back->pop();
			}
			delete back;
		}
		void createArray(int capacity)
		{
			tarray<int>* arrays = new tarray<int> [capacity];
			//用于清理时候
			this->back->push(arrays);
			for (int i = 0; i < capacity; i++)
			{
				this->pool->push(arrays + i);
			}
		}
		/**/
		inline void recycle(tarray<int>* _array)
		{

			lock.lock(); // @suppress("Method cannot be resolved")
			_array->reset();
			this->pool->push(_array);
			lock.unlock(); // @suppress("Method cannot be resolved")
		}

		/*用于申请大小小于7的int数组块*/
		inline tarray<int>* get()
		{
			tarray<int>* temp;
			lock.lock(); // @suppress("Method cannot be resolved")
			if (pool->jsize() == 0)
			{
				createArray(extCapacity);
			}
			temp = pool->pop();
			lock.unlock(); // @suppress("Method cannot be resolved")
			return temp;
		}
	};
	class parserparam
	{
	private:

		int mark { 128 };
		int length { 128 };
		tarray<int>** listOr { null };

		//是否发生过扩容

		valuespool* pool { null };
	public:
		int size { 0 };bool changed { false };
		vector<tarray<int>*> filter;

		parserparam(valuespool* pool)
		{
			this->pool = pool;
			this->listOr = new tarray<int>*[128];
		}
		~parserparam()
		{
			if (listOr != null)
			{
				delete[] listOr;
			}
		}

		void reset()
		{
			for (int i = 0; i < size; i++)
			{
				this->pool->recycle(listOr[i]);
			}
			this->size = 0;
			this->filter.clear();
		}

		// 对二维数据进行排序，按照元素个数大小进行排序
		inline void bubbleSort(tarray<int>** arr, int length)
		{
			for (int i = 0; i < length - 1; i++)
			{
				bool flag = true;
				for (int j = 0; j < length - 1 - i; j++)
				{
					if (arr[j]->size() > arr[j + 1]->size())
					{
						tarray<int>* v = arr[j];
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
		}
		// 对有序集合判定 有序集合B是否是有序集合A的真子集
		inline bool containAcB(int* a, int alen, int* b, int blen)
		{
			if (alen < blen)
			{
				return false;
			}
			for (int i = 0, j = 0;;)
			{
				if (a[i] < b[j])
				{
					if (++i == alen)
					{
						return false;
					}
				}
				else if (a[i] == b[j])
				{
					if (++j == blen)
					{
						return true;
					}
					else if (++i == alen)
					{
						return false;
					}
				}
				else
				{
					return false;
				}
			}
			return false;
		}

		//将语法解析的最终结果进行排序、除去包含关系后，加入到vector中，vector中set与set之间为OR关系，set中为AND 关系
		inline int toResult(vector<tarray<int>*>& list, tarray<int>** listOr, int listSize)
		{
			//按照元素个数将各个 OR 集合按照 子集的size大小进行排序
			//对每个AND子集按照元素值进行排序
			int startIndex = 0;
			for (int i = 0; i < listSize; i++)
			{
				listOr[i]->sortAndDistinct();
				if (listOr[i]->size() == 0)
				{
					startIndex++;
				}
			}
			bubbleSort(listOr, listSize);

			for (int i = listSize - 1; i >= startIndex; i--)
			{
				tarray<int>* andValues = listOr[i];

				bool contain = false;
				// 如果集合 andValues 存在真子集，则忽略该集合 即 A AND B OR  A 的方式仅需要A即可
				for (int j = startIndex; j < i; j++)
				{
					if (containAcB(andValues->array(), andValues->size(), listOr[j]->array(), listOr[j]->size()))
					{
						contain = true;
						break;
					}
				}
				if (!contain)
				{
					list.push_back(andValues);
				}
				else
				{
					this->pool->recycle(andValues);
				}
			}
			return startIndex;
		}
		vector<tarray<int>*>& getFilter()
		{
			return this->filter;
		}

		void getResult(vector<tarray<int>*>& list)
		{
			this->size = toResult(list, this->listOr, this->size);
		}

		void setFilter(parserparam* param)
		{
			if (this->filter.size() == 0 && param != null)
			{
				param->size = toResult(this->filter, param->listOr, param->size);
			}
		}

		inline void setValue(int value)
		{
			if (value != MIN_INT_VALUE)
			{
				tarray<int>* andVs = this->pool->get();
				andVs->set(value);
				listOr[this->size++] = andVs;
			}
		}

		inline bool contains(int* andVs, int size, int vlaue)
		{
			for (int j = 0; j < size; j++)
			{
				if (andVs[1 + j] == vlaue)
				{
					return true;
				}
			}
			return false;
		}

		inline void ensureCapacity(int newSize)
		{
			if (newSize > length)
			{
				this->changed = true;
				tarray<int>** temp = new tarray<int>*[newSize];
				memmove(temp, this->listOr, sizeof(tarray<int>*) * this->size);
				delete[] this->listOr;
				this->listOr = temp;
				this->length = newSize;
			}
		}
		// 添加 OR value
		inline void addOR(int value)
		{
			if (value != MIN_INT_VALUE)
			{
				ensureCapacity(this->size + 1);
				tarray<int>* andVs = this->pool->get();
				andVs->set(value);
				listOr[this->size++] = andVs;
			}
		}
		// 添加 AND value
		inline void addAND(int param)
		{
			if (param == MIN_INT_VALUE)
			{
				for (int i = 0; i < this->size; i++)
				{
					this->pool->recycle(listOr[i]);
				}
				this->size = 0;
			}
			else
			{
				for (int i = 0; i < this->size; i++)
				{
					this->listOr[i]->addAND(param);
				}
			}
		}

		inline void OR(parserparam* param)
		{
			ensureCapacity(this->size + param->size);
			memmove(this->listOr + this->size, param->listOr, sizeof(int*) * param->size);
			this->size += param->size;
			param->size = 0;
		}

		inline void AND(parserparam* param)
		{
			if (this->size > 0 && param->size > 0)
			{
				ensureCapacity(this->size * param->size);
				//将数据拷贝出来做and运算
				tarray<int>** oldvalue = new tarray<int>*[this->size];
				memmove(oldvalue, this->listOr, sizeof(tarray<int>*) * this->size);
				int listSize = 0;
				for (int i = 0; i < this->size; i++)
				{
					tarray<int>* andVs = oldvalue[i];
					//元素个数
					int size0 = andVs->size();
					if (size0 != 0)
					{
						for (int j = 0; j < param->size; j++)
						{

							if (param->listOr[j]->size() > 0)
							{
								// 生成新的And
								tarray<int>* newAndVs = this->pool->get();
								// 计算交集
								// 先将A集合拷贝至目标集合
								for (int k = 0; k < size0; k++)
								{
									newAndVs->addNew(andVs->at(k));
								}
								// 将B集合拷贝至目标集合，拷贝前，判定目标集合是否已经存在了、是否存在 a AND !a 情况
								tarray<int>* other = param->listOr[j];
								for (int k = 0, size1 = other->size(); k < size1; k++)
								{
									newAndVs->addAND(other->at(k));
								}
								// and操作之后合法，将数据增加到 list中
								this->listOr[listSize++] = newAndVs;
							}
						}
					}
				}
				for (int i = 0; i < this->size; i++)
				{
					this->pool->recycle(oldvalue[i]);
				}
				delete[] oldvalue;
				this->size = listSize;
			}
			else
			{
				for (int i = 0; i < this->size; i++)
				{
					this->pool->recycle(listOr[i]);
				}
				this->size = 0;
			}
		}
	};

	class paramspool
	{
		std::mutex lock; // @suppress("Type cannot be resolved")
		valuespool* vspool;
		jstack<parserparam*>* pool;
	public:
		paramspool()
		{
			this->vspool = new valuespool();
			this->pool = new jstack<parserparam*>(1024);
		}
		~paramspool()
		{
			delete this->vspool;
			while (this->pool->jsize() > 0)
			{
				delete this->pool->pop();
			}
			delete this->pool;
		}

		/**/
		inline void recycle(parserparam* param)
		{
			//如果参数发生过扩容，则直接销毁对象
			if (param->changed)
			{
				param->reset();
				delete param;
			}
			else
			{
				//参数内存大小没有发生过改变，可以回收下次使用
				lock.lock(); // @suppress("Method cannot be resolved")
				param->reset();
				this->pool->push(param);
				lock.unlock(); // @suppress("Method cannot be resolved")
			}
		}

		inline void recycle(tarray<int>* _array)
		{
			this->vspool->recycle(_array);
		}

		/*用于申请大小小于7的int数组块*/
		inline parserparam* get()
		{
			parserparam* temp;
			lock.lock(); // @suppress("Method cannot be resolved")
			// 如果池中存在空余的参数对象，则直接返回
			if (pool->jsize() == 0)
			{
				temp = new parserparam(vspool);
			}
			else
			{
				temp = pool->pop();
			}
			lock.unlock(); // @suppress("Method cannot be resolved")
			return temp;
		}
	};

	class syntaxparser
	{
	private:
		int length { 0 };
		int cursize { 0 };
		int* elements { null };
		int ibracketmark { 0 };
		dictionary* dic { null };
		//分配and存储空间
		//valuespool* vspool{ null };
		//分配参数
		paramspool* pmpool { null };
		vector<tarray<int>*> filter;
		vector<tarray<int>*> result;
		jstack<char> operators;
		jstack<parserparam*> params;
		// 将词条转换为字典对应的值
		inline int getValue(char* chs, int length)
		{
			if (length > 0 && (chs[0] == '!' || chs[0] == '-'))
			{

				int index = dic->get(&chs[1], length - 1);
				return index > 0 ? -index : 0;
			}
			else
			{
				return dic->get(chs, length);
			}
		}

		//将词条转换为整形值，并且将该值加入到数组中
		inline bool addterms(char* chs, int length)
		{
			int value = getValue(chs, length);
			if (cursize == 0 || elements[cursize - 1] == OP_AND || elements[cursize - 1] == OP_OR || elements[cursize - 1] == LEFT)
			{
				addElement(value);
				return true;
			}
			else
			{
				return false;
			}
		}

		//将操作符加入数组
		inline bool addOpType(int opType)
		{
			switch (opType)
			{
				case OP_OR:
				case OP_AND:
					if (cursize > 0 && elements[cursize - 1] != OP_AND && elements[cursize - 1] != OP_OR && elements[cursize - 1] != LEFT)
					{
						addElement(opType);
						return true;
					}
					break;
				default:
					break;
			}
			return false;
		}

		bool addbracket(int bracket)
		{
			switch (bracket)
			{
				case LEFT:
					if (cursize == 0 || elements[cursize - 1] == OP_AND || elements[cursize - 1] == OP_OR || elements[cursize - 1] == LEFT)
					{
						this->ibracketmark++;
						addElement(LEFT);
						return true;
					}
					break;
				case RIGHT:
					if (--this->ibracketmark >= 0 && cursize > 0 && elements[cursize - 1] != OP_AND && elements[cursize - 1] != OP_OR
							&& elements[cursize - 1] != LEFT)
					{
						// 如果参数的上个操作符是左括号，则直接可以把加参数左右两边的括号去掉
						if (this->elements[this->cursize - 2] == LEFT)
						{
							this->elements[this->cursize - 2] = this->elements[this->cursize - 1];
							this->cursize--;
						}
						else
						{
							addElement(RIGHT);
						}
						return true;
					}
					break;
				default:
					break;
			}
			return false;
		}

		inline void addElement(int value)
		{
			if (this->cursize == this->length)
			{
				this->length += 256;
				int* temp = new int[this->length];
				if (elements != null)
				{
					memmove(temp, elements, sizeof(int) * this->cursize);
					delete[] elements;
				}
				this->elements = temp;
			}
			this->elements[this->cursize++] = value;
		}

		inline int opType(int index, char* value)
		{
			if (index > 3 && value[index - 1] == BLANK)
			{
				if (index > 4 && value[index - 2] == 'D' && value[index - 3] == 'N' && value[index - 4] == 'A' && value[index - 5] == BLANK)
				{
					return OP_AND;
				}
				else if (value[index - 2] == 'R' && value[index - 3] == 'O' && value[index - 4] == BLANK)
				{
					return OP_OR;
				}
				else
				{
					return 0;
				}
			}
			else if (index == 0 || (index == 1 && value[index - 1] == BLANK))
			{
				return BLANK;
			}
			else
			{
				return 0;
			}
		}

		inline bool addvalue(int termvalue, int index)
		{

			if (operators.jsize() == 0)
			{
				parserparam* temp = this->pmpool->get();
				temp->setValue(termvalue);
				params.push(temp);
				return true;
			}
			else
			{
				switch (operators.top())
				{
					case AND:
					{
						//如果上一个操作符是AND，那么可以直接将该参数与上一个参数做运算
						operators.pop();
						params.top()->addAND(termvalue);
						break;
					}

					case OR:
					{
						//如果上一个操作符是 OR 且下一个
						if (index + 1 < cursize && elements[index + 1] == OP_OR)
						{

							operators.pop();
							params.top()->addOR(termvalue);
						}
						else
						{
							parserparam* temp0 = this->pmpool->get();
							temp0->setValue(termvalue);
							params.push(temp0);
						}
						break;
					}

					case left_bracket:
					{
						//上个若是左括号则先入栈
						parserparam* temp1 = this->pmpool->get();
						temp1->setValue(termvalue);
						params.push(temp1);
						break;
					}

					default:
						cout << "parser error!" << endl;
						break;
				}
			}
			return true;
		}

		bool rightbracket(bool isEnd)
		{
			parserparam* value = this->params.pop();
			char opType;
			while (this->operators.jsize() > 0 && (opType = this->operators.pop()) != left_bracket)
			{
				switch (opType)
				{
					case OR:
					{
						parserparam* param = this->params.pop();
						value->OR(param);
						this->pmpool->recycle(param);
						break;
					}

					default:
						cout << "parser error!" << endl;
						break;
				}
			}

			while (operators.jsize() > 0 && operators.top() == AND)
			{
				operators.pop();
				parserparam* param = this->params.pop();
				if (isEnd && this->params.jsize() == 0 && param->size && value->size && ((value->size * param->size) > ((value->size + param->size))))
				{
					value->setFilter(param);
				}
				else
				{
					value->AND(param);
				}
				this->pmpool->recycle(param);
			}

			params.push(value);
			return true;
		}

		bool finish()
		{
			if (params.jsize() == 0)
			{
				return false;
			}
			parserparam* value = this->params.pop();
			while (operators.jsize())
			{
				switch (this->operators.pop())
				{
					case OR:
					{
						parserparam* temp = this->params.pop();
						value->OR(temp);
						this->pmpool->recycle(temp);
						break;
					}

					default:
						cout << "parser error!" << endl;
						break;
				}
			}
			if (operators.jsize() || params.jsize())
			{
				this->pmpool->recycle(value);
				return emptyparam();
			}
			value->getResult(this->result);
			this->filter = value->getFilter();
			this->pmpool->recycle(value);
			return true;
		}

		bool emptyparam()
		{
			while (params.jsize())
			{
				this->pmpool->recycle(this->params.pop());
			}
			return false;
		}

		/*语法验证*/
		char prepare(const string& text, int& process_index)
		{
			uint termindex = 0;
			uint textlen = text.length();
			char termChars[128];
			char value[256];
			int index = 0;
			const char* str = text.c_str();
			for (process_index = 0; process_index < (int) textlen; process_index++)
			{
				char chvl = str[process_index];
				switch (chvl)
				{
					case right_bracket:
					{
						switch (opType(index, value))
						{
							case OP_OR:
							case OP_AND:
								this->cursize = 0;
								return error_term_less;
								break;
							case BLANK: //BLANK
								break;
							default:
								if (termindex > 0)
								{
									if (!this->addterms(termChars, termindex))
									{
										this->cursize = 0;
										return error_operator;
									}
								}
								break;
						}
						termindex = 0;
						index = 0;
						value[index++] = BLANK;
						this->addbracket(RIGHT);
						break;
					}

					case left_bracket:
					{
						if (process_index > 0 && value[index - 1] != BLANK)
						{
							value[index++] = BLANK;
						}
						int type = opType(index, value);
						switch (type)
						{
							case OP_OR:
							case OP_AND:
								if (!this->addOpType(type))
								{
									this->cursize = 0;
									return error_operator;
								}
								break;
							case BLANK: //BLANK
								break;
							default: // data
								this->cursize = 0;
								return error_operator;
								break;
						}
						termindex = 0;
						index = 0;
						value[index++] = BLANK;
						this->addbracket(LEFT);
						break;
					}

					case BLANK:
					{
						if (index == 0 || (index > 0 && value[index - 1] != BLANK))
						{
							value[index++] = BLANK;
						}

						int type = opType(index, value);

						switch (type)
						{
							case OP_OR:
							case OP_AND:
								if (!this->addOpType(type))
								{
									this->cursize = 0;
									return error_operator;
								}
								break;
							case BLANK: //BLANK
								break;
							default:
								if (termindex > 0)
								{
									if (!this->addterms(termChars, termindex))
									{
										this->cursize = 0;
										return error_operator;
									}
								}
								break;
						}
						termindex = 0;
						index = 0;
						value[index++] = BLANK;
						break;
					}

					default:
					{
						if (termindex >= 127)
						{
							this->cursize = 0;
							return error_term_tolong;
						}
						value[index++] = chvl;
						termChars[termindex++] = chvl;
						if (process_index == (int) (textlen - 1))
						{
							if (termindex > 0)
							{
								if (!this->addterms(termChars, termindex))
								{
									this->cursize = 0;
									return error_operator;
								}
							}
						}
						break;
					}
				}
			}
			if (this->ibracketmark == 0 && (this->cursize == 0 || (this->elements[this->cursize - 1] != OP_AND && this->elements[this->cursize - 1] != OP_OR)))
			{
				return 1;
			}
			else
			{
				return error_bracket;
			}
		}
	public:
		syntaxparser(dictionary* dic, paramspool* pmpool)
		{
			this->dic = dic;
			this->pmpool = pmpool;
		}

		~syntaxparser()
		{
			if (this->elements != null)
			{
				delete[] this->elements;
				this->elements = null;
			}
			for (auto andVs : this->result)
			{
				this->pmpool->recycle(andVs);
			}
			for (auto andVs : this->filter)
			{
				this->pmpool->recycle(andVs);
			}
		}

		vector<tarray<int>*>& getFilter()
		{
			return this->filter;
		}

		vector<tarray<int>*>& getResult()
		{
			return this->result;
		}

		void print()
		{
			cout << "size:" << this->cursize << endl;
			for (int i = 0; i < this->cursize; i++)
			{
				switch (elements[i])
				{

					case LEFT:
						cout << "(";
						break;
					case RIGHT:
						cout << ")";
						break;
					case OP_OR:
						cout << " OR ";
						break;
					case OP_AND:
						cout << " AND ";
						break;
					default:
						string term;
						if (elements[i] < 0)
						{
							cout << "!";
							dic->get(-elements[i], term);
						}
						else
						{
							dic->get(elements[i], term);
						}
						cout << term;
						break;
				}
			}
			cout << endl;
		}
		//如果语法解析错误，parserindex为遇到错误的位置
		bool parser(const string& text, int& process_index)
		{
			char resultcode = prepare(text, process_index);
			if (resultcode == 1)
			{
				for (int i = 0; i < cursize; i++)
				{
					switch (elements[i])
					{
						case LEFT:
							this->operators.push(left_bracket);
							break;
						case RIGHT:
							this->rightbracket(cursize == (i + 1));
							break;
						case OP_OR:
							this->operators.push(OR);
							break;
						case OP_AND:
							this->operators.push(AND);
							break;
						default:
							this->addvalue(elements[i], i);
							break;
					}
				}
				return this->finish();
			}
			return false;
		}
	};
}
#endif
