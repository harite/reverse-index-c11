/*
 * filestream.h
 *  文件读写缓冲类
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef  FILESTREAM_H_
#define  FILESTREAM_H_
#pragma once
#include<fstream>
#include<iostream>
#include <cstdlib> 
namespace qstardb
{
	using namespace std;
	static const int RW_BUF_SIZE = 1048576;
	typedef long long int64;
	class charreader
	{
	private:
		char* buf;
		int offset;
		int capacity;
	public:
		charreader(char* buf, int pos, int len)
		{
			this->offset = pos;
			this->buf = buf;
			this->capacity = capacity;
		}
		inline char readChar()
		{
			if (ensureCapacity(1))
			{
				this->capacity--;
				return buf[offset++];
			}
			else
			{
				cout << "read char error!" << endl;
				return -1;
			}
		}
		inline short readShort()
		{
			if (ensureCapacity(2))
			{
				return readChar() << 8 | (short) (readChar() & 0xff);
			}
			else
			{
				cout << "read short error!" << endl;
				return -1;
			}
		}

		inline int readInt()
		{
			if (ensureCapacity(4))
			{
				return readShort() << 16 | (int) (readShort() & 0xffff);
			}
			else
			{
				cout << "read int error!" << endl;
				return -1;
			}
		}

		int64 readInt64()
		{
			if (ensureCapacity(8))
			{
				return (((int64) readInt()) << 32) | (((int64) readInt()) & 0xffffffff);
			}
			else
			{
				cout << "read int64 error!" << endl;
				return -1;
			}
		}

		bool ensureCapacity(int size)
		{
			return this->capacity - size >= 0;
		}

		int read(char* ch, int len)
		{
			if ((this->capacity - this->offset) >= len)
			{
				memcpy(ch, buf + this->offset, sizeof(char) * len);
				this->offset += len;
				return len;
			}
			else
			{
				int copylen = this->capacity - this->offset;
				if (copylen > 0)
				{
					memcpy(ch, buf + this->offset, sizeof(char) * copylen);
					this->offset += copylen;
					return copylen;
				}
				return 0;
			}
		}

		~charreader(void)
		{

		}
	};

	class filereader
	{
	private:
		ifstream in;
		int offset{0};
		int bufferSize{0};
		int64 usedSize{0};
		int64 fileLength;
		char* buffer;
	public:
		filereader(string& filename)
		{
			this->buffer = new char[RW_BUF_SIZE];
			this->in.open(filename.c_str(), ios::in | ios::binary);
			this->in.seekg(0, ios_base::end);
			if (this->in.is_open()) 
			{
				this->fileLength = this->in.tellg();
				this->in.seekg(0, ios_base::beg);
			}
			else
			{
				this->fileLength = 0;
			}
		}
		~filereader()
		{
			delete[] this->buffer;
		}
		int64 size()
		{
			return this->fileLength;
		}
		bool isOpen()
		{
			return this->in.is_open();
		}

		inline char readChar()
		{
			if (ensureCapacity(1))
			{
				this->usedSize += 1;
				return buffer[offset++];
			}
			else
			{
				cout << "read char error!" << endl;
				return -1;
			}
		}
		inline short readShort()
		{
			if (ensureCapacity(2))
			{
				return readChar() << 8 | (short) (readChar() & 0xff);
			}
			else
			{
				cout << "read short error!" << endl;
				return -1;
			}
		}

		inline int readInt32()
		{
			if (ensureCapacity(4))
			{
				return readShort() << 16 | (int) (readShort() & 0xffff);
			}
			else
			{
				cout << "read int error!" << endl;
				return -1;
			}
		}

		long long readInt64()
		{
			if (ensureCapacity(8))
			{
				return (((long long) readInt32()) << 32) | ((long long) (readInt32() & 0xffffffff));
			}
			else
			{
				cout << "read int64 error!" << endl;
				return -1;
			}
		}

		bool ensureCapacity(int size)
		{
			if (size > RW_BUF_SIZE)
			{
				cout << "size > RW_BUF_SIZE" << endl;
				return false;
			}
			else if (this->bufferSize - this->offset >= size)
			{
				return true;
			}
			else
			{
				int moveNun = this->bufferSize - this->offset;
				if (moveNun > 0)
				{
					memmove(buffer, buffer + this->offset, sizeof(char) * moveNun);
				}
				this->bufferSize -= this->offset;
				this->offset = 0;
				in.read(buffer + this->bufferSize, RW_BUF_SIZE - this->bufferSize);
				this->bufferSize += in.gcount();
				if (this->bufferSize > size)
				{
					return true;
				}
				else
				{
					cout << "read null" << endl;
					return false;

				}
			}
		}

		void read(char* ch, int len)
		{
			if ((this->bufferSize - this->offset) >= len)
			{
				memcpy(ch, buffer + this->offset, sizeof(char) * len);
				this->offset += len;
				this->usedSize += len;
				return;
			}
			else
			{
				int copylen = this->bufferSize - this->offset;
				if (copylen > 0)
				{
					memcpy(ch, buffer + this->offset, sizeof(char) * copylen);
					this->offset += copylen;
				}
				this->in.read(ch + copylen, len - copylen);
				this->usedSize += len;
				return;
			}
		}

		bool readline(string& line)
		{
			if (getline(in, line))
			{
				this->usedSize += line.length();
				return true;
			}
			else
			{
				return false;
			}
		}

		void close()
		{
			in.close();
		}

		bool hasmore(int64 length)
		{
			return this->usedSize + length <= this->fileLength;
		}

	};

	class charwriter
	{
	private:
		int offset;
		int capacity;

	public:
		char* buffer;
		charwriter(int capacity)
		{
			this->offset = 0;
			this->capacity = capacity;
			this->buffer = new char[capacity];
		}
		charwriter()
		{
			this->offset = 0;
			this->capacity = 16*1024;
			this->buffer = new char[this->capacity];
		}
		int size()
		{
			return offset;
		}
		inline void writeChar(char value)
		{
			this->ensureCapacity(1);
			this->buffer[this->offset++] = value;
		}

		inline void writeShort(short value)
		{
			this->ensureCapacity(2);
			writeChar((char) (value >> 8));
			writeChar((char) value);
		}

		inline void writeInt(int value)
		{
			this->ensureCapacity(4);
			writeShort((short) (value >> 16));
			writeShort((short) value);
		}

		void writeInt64(int64 value)
		{
			this->ensureCapacity(8);
			writeInt((int) (value >> 32));
			writeInt((int) value);
		}

		void write(const char* buf, int len)
		{
			this->write(buf, 0, len);
		}

		void write(const char* buf, int _offset, int len)
		{
			ensureCapacity(len);
			memcpy(this->buffer + this->offset, buf + _offset, sizeof(char) * len);
			this->offset += sizeof(char) * len;

		}

		bool ensureCapacity(int size)
		{
			if (this->offset + size <= this->capacity)
			{
				return true;
			}
			else
			{
				this->capacity = ((this->capacity * 3) >> 1) + size;
				if (this->capacity < this->offset + size)
				{
					this->capacity = this->offset + size;
				}
				char* temp = new char[this->capacity];

				memcpy(temp, this->buffer, this->offset * sizeof(char));

				delete[] this->buffer;
				this->buffer = temp;
			}
			return true;
		}

		~charwriter(void)
		{
			delete[] buffer;
			this->buffer = nullptr;
		}
	};

	class filewriter
	{
	private:
		ofstream out;
		int size;
		char* buffer;
		string file;
	public:
		filewriter(string& filename)
		{
			this->size = 0;
			this->buffer = new char[RW_BUF_SIZE];
			this->file = filename;
			this->out.open(file.c_str(), ios::out | ios::binary);
		}
		bool isOpen()
		{
			return this->out.is_open();
		}
		inline void writeChar(char value)
		{
			this->ensureCapacity(1);
			this->buffer[this->size++] = value;
		}

		inline void writeShort(short value)
		{
			this->ensureCapacity(2);
			writeChar((char) (value >> 8));
			writeChar((char) value);
		}

		inline void writeInt32(int value)
		{
			this->ensureCapacity(4);
			writeShort((short) (value >> 16));
			writeShort((short) value);
		}

		void writeInt64(long long value)
		{
			this->ensureCapacity(8);
			writeInt32((int) (value >> 32));
			writeInt32((int) value);
		}
		void write(const char* buffer, int len)
		{
			this->write(buffer, 0, len);
		}
		void write(const char* buf, int _offset, int len)
		{
			if (RW_BUF_SIZE - this->size < len)
			{
				this->out.write(buffer, this->size);
				this->out.write(buf + _offset, sizeof(char) * len);
				this->size = 0;
			}
			else
			{
				memmove(buffer + this->size, buf + _offset, sizeof(char) * len);
				this->size += sizeof(char) * len;
			}
		}
		void flush()
		{
			if (this->size > 0)
			{
				this->out.write(buffer, this->size);
				this->size = 0;
			}
			this->out.flush();
		}
		void close()
		{
			this->flush();
			this->out.close();
		}
		bool reNameTo(string& newname)
		{
			remove(newname.c_str());
			return rename(this->file.c_str(), newname.c_str());
		}
		bool ensureCapacity(int size)
		{
			if (this->size + size > RW_BUF_SIZE)
			{
				this->flush();
			}
			else
			{
				return true;
			}
			if (this->size + size > RW_BUF_SIZE)
			{
				return false;
			}
			else
			{
				return true;
			}
		}

		~filewriter(void)
		{
			delete[] this->buffer;
			this->buffer = nullptr;
		}
	};
}
#endif
