/*
* nativedici2b.h
*
*  Created on: 2015Äê3ÔÂ11ÈÕ
*      Author: jkuang
*/
#include <map>
#include "nativejni.h"
#include "filestream.h"
#include "rwsyslock.h"
#include "btreei2b.h"
#ifndef _Included_com_eastmoney_reverse_index_jni_BTreeDB
#define _Included_com_eastmoney_reverse_index_jni_BTreeDB
#ifdef __cplusplus
extern "C"
{
#endif
	using namespace std;

	namespace embtree
	{
		class btreemap {

		public:
			btree::block* _block;
			qstardb::rwsyslock lock;
		public:
			btreemap(jint node_max_num, jint data_avg_size)
			{
				this->_block = new btree::block(node_max_num, data_avg_size);
			}
			~btreemap()
			{
				delete this->_block;
			}
		};

		static qstardb::rwsyslock maplock;

		static map<int, btreemap*>* i2bmap = new map<int, btreemap*>();



		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    create
		* Signature: (II)V
		*/
		JNIEXPORT void JNICALL JNICALL Java_com_eastmoney_reverse_index_jni_BTreeDB_create(JNIEnv *, jclass, jint index, jint node_max_num, jint data_avg_size)
		{
			maplock.wrlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				cout << "i2bmap exists,index:" << index << endl;
			}
			else
			{
				(*i2bmap)[index] = new btreemap(node_max_num, data_avg_size);
				cout << "create i2bmap:" << index << endl;
			}
			maplock.unwrlock();
		}

		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    destroy
		* Signature: (I)V
		*/
		JNIEXPORT void JNICALL JNICALL Java_com_eastmoney_reverse_index_jni_BTreeDB_destroy(JNIEnv *, jclass, jint index)
		{
			maplock.wrlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				delete (*i2bmap)[index];
				i2bmap->erase(index);
				cout << "destroy i2bmap ,index:" << index << endl;
			}
			else
			{
				cout << "i2bmap not exists:" << index << endl;
			}
			maplock.unwrlock();

		}

		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    put
		* Signature: (IJ[B)I
		*/
		JNIEXPORT jint JNICALL JNICALL Java_com_eastmoney_reverse_index_jni_BTreeDB_put(JNIEnv * env, jclass, jint index, jlong key, jbyteArray value)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{

				int length = env->GetArrayLength(value);
				jboolean copy1 = false;
				signed char* ch = env->GetByteArrayElements(value, &copy1);
				(*i2bmap)[index]->lock.wrlock();
				(*i2bmap)[index]->_block->insert(key, (const char*)ch, length);
				(*i2bmap)[index]->lock.unwrlock();
				env->ReleaseByteArrayElements(value, ch, 0);
				maplock.unrdlock();
				return 1;
			}
			maplock.unrdlock();
			return 0;
		}

		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    get
		* Signature: (IJ)[B
		*/
		JNIEXPORT jbyteArray JNICALL JNICALL Java_com_eastmoney_reverse_index_jni_BTreeDB_get(JNIEnv * env, jclass, jint index, jlong key)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int vlength = 0;
				(*i2bmap)[index]->lock.rdlock();
				const char* result = (*i2bmap)[index]->_block->find(key, vlength);
				jbyteArray data = createJByteArray(env, result, vlength);
				(*i2bmap)[index]->lock.unrdlock();
				maplock.unrdlock();
				return data;
			}
			maplock.unrdlock();
			return createJByteArray(env, nullptr, 0);
		}
		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    remove
		* Signature: (IJ)[B
		*/
		JNIEXPORT jint JNICALL JNICALL Java_com_eastmoney_reverse_index_jni_BTreeDB_remove(JNIEnv * env, jclass, jint index, jlong key)
		{

			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				(*i2bmap)[index]->lock.wrlock();
				(*i2bmap)[index]->_block->remove(key);
				(*i2bmap)[index]->lock.unwrlock();
				maplock.unrdlock();
				return 1;
			}
			maplock.unrdlock();
			return 0;
		}
	}
#ifdef __cplusplus
}
#endif
#endif
