#pragma once
#ifndef BTREEI2B_H_
#define BTREEI2B_H_
#include <string.h>
#include "filestream.h"
#include "rwsyslock.h"
namespace btree
{
	typedef long long int64;
	//临时共享空间，用于数据垃圾清理临时存储数据
	static const int  SHARD_SIZE = 1024 * 1024;
	static const int  HEAD_NODE = 0X12D21;
	static const int  TAIL_NODE = 0X12F21;
	static const int  HEAD_PAGE = 0X12345;
	static const int  TAIL_PAGE = 0X54321;
	static const int  HEAD_BLOCK = 0X12A21;
	static const int  TAIL_BLOCK = 0X12B21;


	int64 currentTimeMillis()
	{
		auto time_now = chrono::system_clock::now();
		auto duration_in_ms = chrono::duration_cast<chrono::milliseconds>(time_now.time_since_epoch());
		return duration_in_ms.count();
	}

	enum type
	{
		type_insert, type_index, type_ceil, type_floor
	};
	class node
	{
	public:
		int64 key{ 0 };
		int offset{ 0 };
		int length{ 0 };
		void set(int64 key, int offset, int length)
		{
			this->key = key;
			this->offset = offset;
			this->length = length;
		}
	};
	class page
	{
	private:
		//元素空间总长度
		int nodeLength{ 1024 };
		//数据空间总长度
		int dataLength{ 1024 * 128 };
		//数据交换空间，临时数据空间
		char* shardData;
		//临时数据空间长度
		int shardLength{ 0 };
	public:
		//已经使用的数据
		int usedDataSize{ 0 };
		//元素个数
		int nodeSize{ 0 };
		//数据已经被删除
		int delDataSize{ 0 };
		//node节点，记录数据的主键，value存储的偏移地址以及长度大小
		node* nodes;
		//数据存放空间
		char* datas;
		int dataSize()
		{
			return this->usedDataSize - this->delDataSize;
		}
	public:
		page(int nodelength, int datalength, char* shardData, int shardLength)
		{
			this->nodeLength = nodelength;
			this->dataLength = datalength;
			this->shardData = shardData;
			this->shardLength = shardLength;
			this->nodes = new node[this->nodeLength];
			this->datas = new char[this->dataLength];
		}
		~page()
		{
			delete[] this->nodes;
			delete[] this->datas;
		}
		inline int size()
		{
			return this->nodeSize;
		}
		inline int compare(int64 key1, int64 key2)
		{
			return key1 == key2 ? 0 : (key1 > key2 ? 1 : -1);
		}

		void ensureCapacity(int nodeCapacity, int dataCapacity)
		{
			int oldCapacity = this->nodeLength;
			int minCapacity = this->nodeSize + nodeCapacity;
			//如果节点数量不足，则需要扩容
			if (minCapacity > oldCapacity)
			{
				int extcapacity = oldCapacity >> 1;
				this->nodeLength = oldCapacity + (extcapacity > 256 ? 256 : extcapacity) + 32;
				//扩容量最大为256+32条数据的空间
				if (this->nodeLength < minCapacity)
				{
					this->nodeLength = minCapacity;
				}
				node* temp = new node[this->nodeLength];
				memmove(temp, nodes, sizeof(node) * this->nodeSize);
				delete[] this->nodes;
				this->nodes = temp;
			}

			int oldLength = this->dataLength;
			int minDataCapacity = this->usedDataSize + dataCapacity;
			//数据存储空间不足，需要创新分配容量
			if (minDataCapacity > oldLength)
			{
				//如果被已删除的占位数据空间大于需要分配的空间，则清理空间
				if (this->delDataSize >= dataCapacity)
				{
					//使用共享空间做为临时存储
					bool useShardData = this->usedDataSize < this->shardLength;
					//临时存储数据空间的地址
					char* tempData = useShardData ? shardData : new char[this->usedDataSize];
					memmove(tempData, this->datas, sizeof(char) * this->usedDataSize);
					this->delDataSize = 0;
					this->usedDataSize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < this->nodeSize; i++)
					{
						memmove(datas + this->usedDataSize, tempData + this->nodes[i].offset, sizeof(char) * this->nodes[i].length);
						this->nodes[i].set(this->nodes[i].key, this->usedDataSize, this->nodes[i].length);
						this->usedDataSize += this->nodes[i].length;
					}
					//如果不是使用的共享空间，则释放临时内存
					if (!useShardData)
					{
						delete[] tempData;
					}
				}
				else {//直接申请新的存储空间，进行数据扩容
					int usedCapacity = this->usedDataSize - this->delDataSize;
					int extcapacity = usedCapacity >> 1;
					this->dataLength = usedCapacity + (extcapacity > 64 * 1024 ? 64 * 1024 : extcapacity);
					if (this->dataLength < usedCapacity + dataCapacity)
					{
						this->dataLength = usedCapacity + dataCapacity;
					}
					char* tempData = new char[this->dataLength];
					//重新申请了内存就重新清理
					this->delDataSize = 0;
					this->usedDataSize = 0;
					//将数据拷贝回原来的地址
					for (int i = 0; i < this->nodeSize; i++)
					{
						memcpy(tempData + this->usedDataSize, datas + nodes[i].offset, sizeof(char) * this->nodes[i].length);
						this->nodes[i].set(this->nodes[i].key, this->usedDataSize, this->nodes[i].length);
						this->usedDataSize += this->nodes[i].length;
					}
					delete[] this->datas;
					this->datas = tempData;
				}
			}
		}

		inline int indexof(node* nodes, int64 key, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = compare(nodes[mid].key, key);
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
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

		bool insert(int64 key, const char* ch, int length)
		{
			if (this->nodeSize == 0 || this->compare(key, nodes[this->nodeSize - 1].key) > 0)
			{
				//确认空间足够
				ensureCapacity(1, length);
				this->nodes[this->nodeSize].set(key, this->usedDataSize, length);
				memmove(this->datas + this->usedDataSize, ch, sizeof(char) * length);
				this->usedDataSize += length;
				this->nodeSize++;
				return true;
			}
			else if (this->compare(key, nodes[0].key) < 0)
			{
				ensureCapacity(1, length);
				memmove(this->nodes + 1, this->nodes, sizeof(node) * this->nodeSize);
				this->nodes[0].set(key, this->usedDataSize, length);
				memmove(this->datas + this->usedDataSize, ch, sizeof(char) * length);
				this->usedDataSize += length;
				this->nodeSize++;
				return true;
			}
			else
			{
				int pos = indexof(this->nodes, key, this->nodeSize, type_insert);
				if (pos >= 0) {
					ensureCapacity(1, length);
					int numMoved = this->nodeSize - pos;
					if (numMoved > 0)
					{
						memmove(this->nodes + pos + 1, this->nodes + pos, sizeof(node) * numMoved);
					}
					this->nodes[pos].set(key, this->usedDataSize, length);
					memmove(this->datas + this->usedDataSize, ch, sizeof(char) * length);
					this->usedDataSize += length;
					this->nodeSize++;
					return true;
				}
				else {//key foud，数据已经存在，则覆盖原数据
					int index = -pos - 1;
					if (this->nodes[index].length >= length)
					{
						if (this->nodes[index].length > length)
							this->delDataSize += this->nodes[index].length - length;
						//数据长度变短了
						this->nodes[index].set(key, this->nodes[index].offset, length);
						memmove(this->datas + this->nodes[index].offset, ch, sizeof(char) * length);
					}
					else
					{
						//空间已经不足，重新分配
						ensureCapacity(0, length);
						this->delDataSize += this->nodes[index].length;
						this->nodes[index].set(key, this->usedDataSize, length);
						memmove(this->datas + this->usedDataSize, ch, sizeof(char) * length);
						this->usedDataSize += length;
					}
					return false;
				}

			}

		}
		const char* find(int64 key, int& length)
		{
			int index = indexof(nodes, key, this->nodeSize, type_index);
			if (index >= 0) {
				length = this->nodes[index].length;
				return this->datas + this->nodes[index].offset;
			}
			else {
				return nullptr;
			}
		}
		inline bool contains(int64 key)
		{
			return indexof(nodes, key, this->nodeSize, type_index) >= 0;
		}

		bool remove(int64 key)
		{
			if (this->nodeSize == 0)
			{
				return false;
			}
			else
			{
				//如果找到数据，则进行操作
				int index = indexof(nodes, key, this->nodeSize, type_index);
				if (index >= 0)
				{
					//标记被删除的数据，讲数据空间纳入回收容量
					this->delDataSize += this->nodes[index].length;
					int moveNum = this->nodeSize - index - 1;
					if (moveNum > 0)
					{
						memmove(this->nodes + index, this->nodes + index + 1, sizeof(node) * moveNum);
					}
					this->nodeSize--;
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		int countDataSize(int from, int to)
		{
			int sum = 0;
			for (int i = from; i < to && i < this->nodeSize; i++)
			{
				sum += this->nodes[i].length;
			}
			return sum;
		}

		void copyTo(int from, int to, page* page) {
			for (int i = from, j = 0; i < to && i < this->nodeSize; i++)
			{
				page->nodes[j++].set(this->nodes[i].key, page->usedDataSize, this->nodes[i].length);
				memmove(page->datas + page->usedDataSize, this->datas + this->nodes[i].offset, sizeof(char) *  this->nodes[i].length);
				page->usedDataSize += this->nodes[i].length;
			}
		}

		page** splitToTwo(bool tail)
		{
			page** pages = new page*[2];
			int mid = (this->nodeSize * (tail ? 7 : 5)) / 10;
			int datasize1 = countDataSize(0, mid) * 11 / 10;
			pages[0] = new page(mid * 11 / 10, datasize1, this->shardData, this->shardLength);
			pages[0]->nodeSize = mid;
			this->copyTo(0, mid, pages[0]);

			int datasize2 = countDataSize(mid, this->nodeSize) * 12 / 10;
			pages[1] = new page((this->nodeSize - mid) * 12 / 10, datasize2, this->shardData, this->shardLength);
			pages[1]->nodeSize = this->nodeSize - mid;
			this->copyTo(mid, this->nodeSize, pages[1]);
			return pages;
		}

		/*判定页的范围是否包含 key*/
		int rangecontains(int64 key)
		{
			if (this->nodeSize == 0)
			{
				return 0;
			}
			if (this->compare(this->nodes[this->nodeSize - 1].key, key) < 0)
			{
				return -1;
			}
			else if (this->compare(this->nodes[0].key, key) > 0)
			{
				return 1;
			}
			return 0;
		}

		void write(filewriter& writer)
		{
			//读取页数量
			writer.writeInt32(this->nodeSize);
			for (int i = 0; i < nodeSize; i++)
			{
				writer.writeInt32(HEAD_NODE);
				writer.writeInt64(this->nodes[i].key);
				writer.writeInt32(this->nodes[i].length);
				writer.write(this->datas + this->nodes[i].offset, this->nodes[i].length);
				writer.writeInt32(TAIL_NODE);
			}
		}
	};

	class block
	{
		int size;
		//每个页最大多少条数据
		int max_nodenum_per_page;
		//每条数据的平均大小
		int avg_node_data_size;

		page** pages;
		char* shardData;

		qstardb::rwsyslock lock;
	private:
		inline int indexOf(page** pages, int64 key, int size, type _type)
		{
			int fromIndex = 0;
			int toIndex = size - 1;
			while (fromIndex <= toIndex)
			{
				int mid = (fromIndex + toIndex) >> 1;
				int cmp = pages[mid]->rangecontains(key);
				if (cmp < 0)
					fromIndex = mid + 1;
				else if (cmp > 0)
					toIndex = mid - 1;
				else
					return _type == type_insert ? -(mid + 1) : mid; // key
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
	public:
		block(int max_nodenum_per_page, int avg_node_data_size)
		{
			this->size = 1;
			this->pages = new page*[1];
			this->shardData = new char[SHARD_SIZE];
			this->max_nodenum_per_page = max_nodenum_per_page;
			this->avg_node_data_size = avg_node_data_size;
			this->pages[0] = new page(max_nodenum_per_page / 4, max_nodenum_per_page / 4 * avg_node_data_size, shardData, SHARD_SIZE);
		}
		~block()
		{
			for (int i = 0; i < size; i++)
			{
				delete pages[i];
			}
			delete[] pages;
			delete[] shardData;
		}

		void write(filewriter& writer)
		{
			//读取页数量
			writer.writeInt32(this->size);
			for (int i = 0; i < size; i++)
			{
				writer.writeInt32(HEAD_PAGE);
				this->pages[i]->write(writer);
				writer.writeInt32(TAIL_PAGE);
			}
		}

		inline bool insertAndSplit(int index, int64 key, const char* data, int length)
		{

			bool insertNew = this->pages[index]->insert(key, data, length);
			if (this->pages[index]->size() > this->max_nodenum_per_page)
			{
				page** temp = this->pages[index]->splitToTwo(index == this->size - 1);
				page** newpages = new page*[this->size + 1];
				if (index > 0)
				{
					memmove(newpages, this->pages, sizeof(page*)*index);
				}
				int moveNum = this->size - index - 1;
				if (moveNum > 0)
				{
					memmove(newpages + index + 2, this->pages + index + 1, sizeof(page*) * moveNum);
				}
				newpages[index] = temp[0];
				newpages[index + 1] = temp[1];
				delete pages[index];
				delete[] temp;
				delete[] this->pages;
				this->pages = newpages;
				this->size++;
			}
			return insertNew;
		}

		bool insert(int64 key, const char* data, int length)
		{
			bool result = false;
			lock.wrlock();
			if (this->size == 1 || this->pages[0]->rangecontains(key) >= 0)
			{
				result = this->insertAndSplit(0, key, data, length);
			}
			else if (this->pages[this->size - 1]->rangecontains(key) <= 0)
			{
				result = this->insertAndSplit(this->size - 1, key, data, length);
			}
			else
			{
				int pageno = indexOf(this->pages, key, this->size, type_ceil);
				result = this->insertAndSplit(pageno, key, data, length);
			}
			lock.unwrlock();
			return result;
		}

		bool combine(int index0, int index1)
		{
			page* temp0 = this->pages[index0];
			page* temp1 = this->pages[index1];
			int size0 = temp0->size();
			int size1 = temp1->size();
			int datasize0 = temp0->dataSize();
			int datasize1 = temp1->dataSize();
			page* newpage = new page(size0 + size1, datasize0 + datasize1, shardData, SHARD_SIZE);

			if (temp0->delDataSize > 0)
			{
				for (int i = 0; i < temp0->nodeSize; i++)
				{
					newpage->nodes[i].set(temp0->nodes[i].key, newpage->usedDataSize, temp0->nodes[i].length);
					memmove(newpage->datas + newpage->usedDataSize, temp0->datas + temp0->nodes[i].offset, sizeof(char)*temp0->nodes[i].length);
					newpage->usedDataSize += temp0->nodes[i].length;
				}
			}
			else
			{
				memmove(newpage->datas + newpage->usedDataSize, temp0->datas, sizeof(char)*temp0->usedDataSize);
				newpage->usedDataSize += temp0->usedDataSize;
			}
			for (int i = 0; i < temp1->nodeSize; i++)
			{
				newpage->nodes[i + temp0->nodeSize].set(temp1->nodes[i].key, newpage->usedDataSize, temp1->nodes[i].length);
				memmove(newpage->datas + newpage->usedDataSize, temp1->datas + temp1->nodes[i].offset, sizeof(char)*temp1->nodes[i].length);
				newpage->usedDataSize += temp1->nodes[i].length;
			}
			//设置新也的数据量
			newpage->nodeSize = temp0->nodeSize + temp1->nodeSize;
			//释放原数据页1
			delete temp0;
			//释放原数据页2
			delete temp1;
			//将新页数据放到 index0的位置
			this->pages[index0] = newpage;
			//将空白页删除
			int moveNum = this->size - index1 - 1;
			if (moveNum > 0)
			{
				memmove(this->pages + index1, this->pages + index1 + 1, sizeof(page*)*moveNum);
			}
			this->size--;
			return true;
		}

		bool combine(int index)
		{
			if (this->size == 1)
			{
				return false;
			}
			//如果数据页内的数据小于规定最大数据量的32分之一，则合并数据页
			else if (this->pages[index]->size() < (this->max_nodenum_per_page / 64))
			{
				//如果是第一页，则将第二页的合并到第一页
				if (index == 0)
				{
					combine(0, 1);
				}
				else//其他情况则将数据将与前一页合并
				{
					combine(index - 1, index);
				}
				return true;
			}
			else
			{
				return false;
			}
		}



		bool remove(int64 key)
		{

			bool result = false;
			lock.wrlock();
			if (this->pages[this->size - 1]->rangecontains(key) == 0)
			{
				result = this->pages[this->size - 1]->remove(key);
				this->combine(this->size - 1);
			}
			else
			{
				int pageno = indexOf(this->pages, key, this->size, type_index);
				if (pageno >= 0)
				{
					result = this->pages[pageno]->remove(key);
					this->combine(pageno);
				}
			}
			lock.unwrlock();
			return result;
		}

		bool find(int64 key, string& str)
		{
			bool result = false;
			lock.rdlock();
			int pageno = indexOf(this->pages, key, this->size, type_index);
			if (pageno >= 0)
			{
				int length = 0;
				const char* temp = this->pages[pageno]->find(key, length);
				if (temp != nullptr) {
					str.append(temp, length);
					result = true;
				}
			}
			lock.unrdlock();
			return result;
		}
		bool find(int64 key, qstardb::charwriter& writer)
		{
			bool result = false;
			lock.rdlock();
			int pageno = indexOf(this->pages, key, this->size, type_index);
			if (pageno >= 0)
			{
				int length = 0;
				const char* temp = this->pages[pageno]->find(key, length);
				if (temp != nullptr) {
					writer.writeInt(length);
					writer.write(temp, length);
					result = true;
				}
			}
			lock.unrdlock();
			return result;
		}
	};

	class concurrentmap
	{
	private:
		int partition;
		block** blocks;
		inline int _hash(int64 key)
		{
			int _h = key % this->partition;
			return _h > 0 ? _h : -_h;
		}
	public:
		concurrentmap(int partition, int max_nodenum_per_page, int avg_node_data_size)
		{
			this->partition = partition;
			this->blocks = new block*[this->partition];
			for (int i = 0; i < this->partition; i++)
			{
				this->blocks[i] = new block(max_nodenum_per_page, avg_node_data_size);
			}
		}
		~concurrentmap()
		{
			for (int i = 0; i < this->partition; i++)
			{
				delete this->blocks[i];
			}
			delete[] this->blocks;
		}

		void insert(int64 key, const char* ch, int length)
		{
			this->blocks[_hash(key)]->insert(key, ch, length);
		}

		void remove(int64 key)
		{
			this->blocks[_hash(key)]->remove(key);
		}

		bool find(int64 key, string& word)
		{
			return this->blocks[_hash(key)]->find(key, word);
		}

		bool find(int64 key, qstardb::charwriter& writer)
		{
			return this->blocks[_hash(key)]->find(key, writer);
		}
		void write(filewriter& writer)
		{
			//标记起始
			writer.writeInt64(HEAD_MARK);
			//当前时间戳
			writer.writeInt64(currentTimeMillis());
			//分区数量
			writer.writeInt32(this->partition);
			for (int i = 0; i < partition; i++)
			{
				writer.writeInt32(HEAD_BLOCK);
				this->blocks[i]->write(writer);
				writer.writeInt32(TAIL_BLOCK);
			}
			writer.writeInt64(TAIL_MARK);
		}

		void dump(string& filename)
		{
			string temp = filename;
			temp.append(".temp");
			filewriter writer(temp);
			write(writer);
			writer.flush();
			writer.close();
			//临时文件写完以后，将临时文件重名为正式名称
			writer.reNameTo(filename);
			cout << " end of dump ,file name:" << filename << endl;
		}


		bool checkNode(filereader& reader)
		{
			char temp[1024];
			//长度是否足够 头部校验码 4字节、数据主键 8字节、数据长度4字节
			if (reader.hasmore(16) && reader.readInt32() == HEAD_NODE) {
				/*int64 key =*/ reader.readInt64();
				int length = reader.readInt32();
				//数据长度是否足够
				if (length >= 0 && reader.hasmore(length)) {
					//循环把数据读取完毕
					while (length > 0)
					{
						if (length - 1024 > 0) {
							reader.read(temp, 1024);
							length -= 1024;
						}
						else {
							reader.read(temp, length);
							break;
						}
					}
					//单条数据的结尾是否等于标志符
					if (reader.hasmore(4) && reader.readInt32() == TAIL_NODE)
					{
						//数据校验OK
						return true;
					}
					else
					{
						return false;
					}
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
		bool checkPage(filereader& reader)
		{
			if (reader.hasmore(8) && reader.readInt32() == HEAD_PAGE)
			{
				int nodenum = reader.readInt32();
				for (int i = 0; i < nodenum; i++)
				{
					if (!checkNode(reader))
					{
						return false;
					}
				}
				if (reader.hasmore(4) && reader.readInt32() == TAIL_PAGE)
				{
					//数据校验成功
					return true;
				}
				else {
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		bool checkBlock(filereader& reader)
		{
			if (reader.hasmore(8) && reader.readInt32() == HEAD_BLOCK)
			{

				int pagenum = reader.readInt32();
				for (int i = 0; i < pagenum; i++)
				{
					if (!checkPage(reader))
					{
						return false;
					}
				}
				if (reader.hasmore(4) && reader.readInt32() == TAIL_BLOCK) {
					cout << " block is ok!" << endl;
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
		bool checkfile(filereader& reader)
		{
			if (reader.hasmore(20) && HEAD_MARK == reader.readInt64())
			{
				//读取文件时间
				int64 time = reader.readInt64();
				if (currentTimeMillis() - time > 86400l * 1000l * 3)
				{
					cout << "file out of time" << endl;
					return false;
				}
				int partition = reader.readInt32();
				if (partition < 1 || partition >32 * 1024)
				{
					cout << "partition error! value:" << partition << endl;

					return false;
				}

				for (int i = 0; i < partition; i++)
				{
					if (!checkBlock(reader)) {

						return false;
					}
				}
				if (reader.hasmore(8) && reader.readInt64() == TAIL_MARK) {

					cout << "file is ok:" << endl;
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		bool checkfile(string& filename)
		{
			filereader reader(filename);
			bool result = checkfile(reader);
			reader.close();
			return result;
		}


		bool loadNode(filereader& reader, char* temp, int tempLength)
		{
			//长度是否足够 头部校验码 4字节、数据主键 8字节、数据长度4字节
			if (reader.hasmore(16) && reader.readInt32() == HEAD_NODE) {
				int64 key = reader.readInt64();
				int length = reader.readInt32();
				//数据长度是否足够
				if (length >= 0 && reader.hasmore(length)) {

					if (length <= tempLength)
					{
						reader.read(temp, length);
						this->insert(key, temp, length);
					}
					else
					{
						char* data = new char[length];
						reader.read(data, length);
						this->insert(key, data, length);
					}
					//单条数据的结尾是否等于标志符
					if (reader.hasmore(4) && reader.readInt32() == TAIL_NODE)
					{
						//数据校验OK
						return true;
					}
					else
					{
						return false;
					}
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
		bool loadPage(filereader& reader)
		{
			if (reader.hasmore(8) && reader.readInt32() == HEAD_PAGE)
			{
				int tempLength = 1024 * 1024;
				char* temp = new char[tempLength];
				int nodenum = reader.readInt32();
				for (int i = 0; i < nodenum; i++)
				{
					if (!loadNode(reader, temp, tempLength))
					{
						delete[] temp;
						return false;
					}
				}
				delete[] temp;
				if (reader.hasmore(4) && reader.readInt32() == TAIL_PAGE)
				{
					//数据校验成功
					return true;
				}
				else {
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		bool loadBlock(filereader& reader) {
			if (reader.hasmore(8) && reader.readInt32() == HEAD_BLOCK)
			{

				int pagenum = reader.readInt32();
				for (int i = 0; i < pagenum; i++)
				{
					if (!loadPage(reader))
					{
						return false;
					}
				}
				if (reader.hasmore(4) && reader.readInt32() == TAIL_BLOCK) {
					cout << " block is ok!" << endl;
					return true;
				}
				else {
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		bool load(string& filename)
		{
			if (checkfile(filename))
			{
				filereader reader(filename);
				//校验文件头是否正确
				if (reader.hasmore(20) && HEAD_MARK == reader.readInt64())
				{
					//读取文件时间
					int64 time = reader.readInt64();
					if (currentTimeMillis() - time > 86400l * 1000l * 3)
					{
						cout << "file out of time" << endl;
						reader.close();
						return false;
					}
					int partition = reader.readInt32();
					if (partition < 1 || partition >32 * 1024)
					{
						cout << "partition error! value:" << partition << endl;
						reader.close();
						return false;
					}

					for (int i = 0; i < partition; i++)
					{
						if (!loadBlock(reader)) {
							reader.close();
							return false;
						}
					}
					if (reader.hasmore(8) && reader.readInt64() == TAIL_MARK) {
						reader.close();
						cout << "local file ok:" << filename << endl;
						return true;
					}
					else
					{
						reader.close();
						return false;
					}
				}
				else
				{
					reader.close();
					return false;
				}
			}
			else
			{
				return false;
			}
		}

	};
}
#endif
