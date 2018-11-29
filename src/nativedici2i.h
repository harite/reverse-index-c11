/*
 * nativedici2i.h
 *
 *  Created on: 2015年3月11日
 *      Author: jkuang
 */
#include "dicmapi2i.h"
#ifndef _Included_org_jkuang_qstardb_I2IMap
#define _Included_org_jkuang_qstardb_I2IMap
#ifdef __cplusplus
extern "C"
{
#endif
	namespace dici2imap
	{
		typedef long long int64;

		static qstardb::rwsyslock i2ilock;

		static map<int, mapi2i::dici2i<int64, int64>*>* intmap = new map<int, mapi2i::dici2i<int64, int64>*>();

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_create(JNIEnv *, jclass, jint index, jint mode)
		{
			i2ilock.wrlock();
			if (intmap->find(index) != intmap->end())
			{
				cout << "intmap exists,index:" << index << endl;
			}
			else
			{
				(*intmap)[index] = new mapi2i::dici2i<int64, int64>(mode);
				cout << "create intmap:" << index << endl;
			}
			i2ilock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_destroy(JNIEnv *, jclass, jint index)
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
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_put(JNIEnv *, jclass, jint index, jlong key, jlong value)
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
		JNIEXPORT jlong JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_get(JNIEnv *, jclass, jint index, jlong key)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				int64 value;
				bool result = (*intmap)[index]->get(key, value);
				i2ilock.unrdlock();
				if (result)
				{
					return value;
				}
				else
				{
					return qstardb::MIN_VALUE;
				}
			}
			i2ilock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2IMap
		 * Method:    remove
		 * Signature: (IJ)[B
		 */
		JNIEXPORT jlong JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_remove(JNIEnv *, jclass, jint index, jlong key)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				int64 oldvalue;
				bool result = (*intmap)[index]->remove(key, oldvalue);
				if (result)
				{
					i2ilock.unrdlock();
					return oldvalue;
				}
			}
			i2ilock.unrdlock();
			return qstardb::MIN_VALUE;

		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_load(JNIEnv * env, jclass, jint index, jstring file)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*intmap)[index]->readfile(filename);
				env->ReleaseStringUTFChars(file, ch);
				i2ilock.unrdlock();
				return true;
			}
			i2ilock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2IMap_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			i2ilock.rdlock();
			if (intmap->find(index) != intmap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*intmap)[index]->writefile(filename);
				env->ReleaseStringUTFChars(file, ch);
				i2ilock.unrdlock();
				return true;
			}
			i2ilock.unrdlock();
			return false;
		}

	}
#ifdef __cplusplus
}
#endif
#endif
