/*
 * nativedici2i.h
 *
 *  Created on: 2015年3月11日
 *      Author: jkuang
 */
#include "elastici2i.h"
#ifndef _Included_org_jkuang_qstardb_I2IMap
#define _Included_org_jkuang_qstardb_I2IMap
#ifdef __cplusplus
extern "C"
{
#endif
	namespace elasticsmap
	{
		typedef long long int64;

		static qstardb::rwsyslock i2ilock;

		static map<int, emapi2i<int64, int64>*>* intmap = new map<int, emapi2i<int64, int64>*>();

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_I2IMap_create(JNIEnv *, jclass, jint index, jint part)
		{
			i2ilock.wrlock();
			if (intmap->find(index) != intmap->end())
			{
				cout << "intmap exists,index:" << index << endl;
			}
			else
			{
				(*intmap)[index] = new emapi2i<int64, int64>(part);
				cout << "create intmap:" << index << endl;
			}
			i2ilock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_I2IMap_destroy(JNIEnv *, jclass, jint index)
		{
			i2ilock.wrlock();
			if (intmap->find(index) != intmap->end())
			{
				delete (*intmap)[index];
				intmap->erase(index);
				cout << "destroy intmap ,index:" << index << endl;
			}
			else
			{
				cout << "intmap not exists:" << index << endl;
			}
			i2ilock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    put
		 * Signature: (IJJ)I
		 */
		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_I2IMap_put(JNIEnv *, jclass, jint index, jlong key, jlong value)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				(*intmap)[index]->add(key, value);
				i2ilock.unrdlock();
				return 1;
			}
			i2ilock.unrdlock();
			return 0;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    get
		 * Signature: (IJ)[B
		 */
		JNIEXPORT jlong JNICALL Java_org_jkuang_qstar_commons_jni_I2IMap_get(JNIEnv *, jclass, jint index, jlong key)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				int64 value =qstardb::MIN_VALUE;
				(*intmap)[index]->get(key, value);
				i2ilock.unrdlock();
				return value;
			}
			i2ilock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    remove
		 * Signature: (IJ)[B
		 */
		JNIEXPORT jlong JNICALL Java_org_jkuang_qstar_commons_jni_I2IMap_remove(JNIEnv *, jclass, jint index, jlong key)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				bool result = (*intmap)[index]->remove(key);
				if (result)
				{
					i2ilock.unrdlock();
					return 0;
				}
			}
			i2ilock.unrdlock();
			return qstardb::MIN_VALUE;

		}
	}
#ifdef __cplusplus
}
#endif
#endif
