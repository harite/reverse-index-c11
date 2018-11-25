#include "jni.h"
#include "nativedics2s.h"
#include "nativedici2b.h"
#include "nativedici2i.h"
#include "nativeintset.h"
#include "nativeindex.h"
#include "nativebase.h"
#include<sstream>
#include <string.h>
#include <atomic>
#include <array>
using namespace std;
using namespace qstardb;
// @suppress("Symbol is not resolved")
// @suppress("Symbol is not resolved")
string tostring(int64 i)
{
	stringstream s;
	s << i;
	return s.str();
}

bool read1(filereader& reader, dictionary* dic)
{
	reader.readChar();
	int size = reader.readInt32();
	char temp[128];
	for (int i = 0; i < size; i++)
	{
		char len = reader.readChar();
		reader.read(temp, len);
		string term(temp, len);
		dic->add(term, -1);
	}
	return true;
}
atomic<long long> exeCount { 0 };

reverse::rmindex* add(bool ignoreCase, bool compress, string dumpfile)
{
	reverse::rmindex* index101 = new  reverse::rmindex(ignoreCase, compress);
	index101->load(dumpfile);
	return index101;
}

int test2(string testfile, reverse::rmindex* index101)
{
	filereader reader(testfile);
	long long sum = 0;
	int count = 0;
	vector<string> vs;
	while (true)
	{
		string syntax;
		if (reader.readline(syntax) && syntax.length() > 10)
		{
			vs.push_back(syntax);
		}
		else
		{
			break;
		}
	}
	int size = vs.size();
	auto start = system_clock::now(); // @suppress("Function cannot be resolved")

	while (true)
	{
		string syntax = vs.at(exeCount % size);
		searchstats stat(0, 10, 10);
		index101->query(syntax, 0, 0, true, stat);
		sum += stat.getoffset();
		if (exeCount++ % 100000 == 99999)
		{
			auto end = system_clock::now(); // @suppress("Function cannot be resolved")
			auto duration = duration_cast < milliseconds > (end - start); // @suppress("Symbol is not resolved")
			cout << (100000 * 1000 / (duration.count() + 1)) << " times/s " << " avg:" << duration.count() * 1000 / 100000 << "mis/times offset:" // @suppress("Method cannot be resolved")
					<< endl; // @suppress("Method cannot be resolved") // @suppress("Invalid overload")
			start = system_clock::now();
		}

	}
	return count;
}

int test3(string testfile, reverse::rmindex* index101, string outfile)
{
	filereader reader(testfile);
	int count = 0;
	filewriter writer(outfile);

	while (true)
	{
		string syntax;
		count++;
		if (reader.readline(syntax) && syntax.length() > 10)
		{
			searchstats stat(100, 100, 30);
			index101->query(syntax, 0, 0, true, stat);
			string test;
			if (count % 1000 == 0)
			{
				cout << "result--" << count << endl;
			}
			for (int64 key : stat.result())
			{
				test.append(tostring(key));
				test.append(",");
			}
			test.append("\n");
			writer.writeBytes(test.c_str(), test.size());
		}
		else
		{
			break;
		}
	}

	writer.close();
	return count;
}

int main()
{
	cout << "---" << sizeof(tarray<int>) << endl;
	string dumpfile("g:/dump/qstardb.101.bin");
	string testfile("g:/dump/1.txt");
	//string testfile("g:/dump/20171020.txt");
	string outfile("d:/dumpbin/out.bin");
	auto start = system_clock::now(); // @suppress("Function cannot be resolved")

	reverse::rmindex* index101 = add(true, false, dumpfile);
	auto end = system_clock::now();

	auto duration = duration_cast < seconds > (end - start);
	cout << "add cost:" << duration.count() << " s" << endl;
	//index101->dump(outfile);
	test2(testfile, index101);
	// std::thread t1(test2,testfile,index101); // @suppress("Type cannot be resolved")
	// std::thread t2(test2,testfile,index101); // @suppress("Type cannot be resolved")
	// std::thread t3(test2, testfile, index101);// @suppress("Type cannot be resolved")
	// std::thread t4(test2, testfile, index101);// @suppress("Type cannot be resolved")
	// t1.join();// @suppress("Type cannot be resolved")
	// t2.join();// @suppress("Type cannot be resolved")
	// t3.join();// @suppress("Type cannot be resolved")
	cout << "--------------------------------" << endl;
	// t4.join();// @suppress("Type cannot be resolved")
	int v; // @suppress("Type cannot be resolved")
	cin >> v;
	return 0;
	//cin >> v;
}

