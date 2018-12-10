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
#include "elastictree.h"
#ifndef _Included_org_jkuang_qstardb_I2BBTREE
#define _Included_org_jkuang_qstardb_I2BBTREE
#ifdef __cplusplus
extern "C"
{
#endif
	using namespace std;
	
	namespace btree
	{
	

		static qstardb::rwsyslock maplock;

		static map<int, concurrentmap*>* i2bmap = new map<int, concurrentmap*>();



		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    create
		* Signature: (II)V
		*/
		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_CurMap_create(JNIEnv *, jclass, jint index,jint part, jint node_max_num,jint data_avg_size)
		{
			maplock.wrlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				cout << "i2bmap exists,index:" << index << endl;
			}
			else
			{
				(*i2bmap)[index] = new concurrentmap(part,node_max_num,data_avg_size);
				cout << "create i2bmap:" << index << endl;
			}
			maplock.unwrlock();
		}

		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    destroy
		* Signature: (I)V
		*/
		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_CurMap_destroy(JNIEnv *, jclass, jint index)
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
		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_CurMap_put(JNIEnv * env, jclass, jint index, jlong key, jbyteArray value)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{

				int length = env->GetArrayLength(value);
				jboolean copy1 = false;
				signed char* ch = env->GetByteArrayElements(value, &copy1);
				(*i2bmap)[index]->insert(key,(const char*) ch, length);
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
		JNIEXPORT jbyteArray JNICALL Java_org_jkuang_qstar_commons_jni_CurMap_get(JNIEnv * env, jclass, jint index, jlong key)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int vlength = 0;
				qstardb::charwriter writer(256);
				if ((*i2bmap)[index]->find(key, writer))
				{
					maplock.unrdlock();
					jbyteArray data = createJByteArray(env, writer.buffer, writer.size());
					return data;
				}
				else {
					maplock.unrdlock();
					return createJByteArray(env, nullptr, 0);
				}
			}
			maplock.unrdlock();
			return createJByteArray(env, nullptr, 0);
		}
		/*
		* Class:     org_jkuang_qstardb_Native_I2BMap
		* Method:    remove
		* Signature: (IJ)[B
		*/
		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_CurMap_remove(JNIEnv * env, jclass, jint index, jlong key)
		{

			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				(*i2bmap)[index]->remove(key);
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
