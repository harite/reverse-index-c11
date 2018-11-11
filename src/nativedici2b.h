/*
 * nativedici2b.h
 *
 *  Created on: 2015年3月11日
 *      Author: jkuang
 */
#include <jni.h>
#include "dicmapi2b.h"
#ifndef _Included_org_jkuang_qstardb_I2BMap
#define _Included_org_jkuang_qstardb_I2BMap
#ifdef __cplusplus
extern "C"
{
#endif
	namespace dici2bmap
	{

		static qstardb::rwsyslock maplock;

		static map<int, mapi2b::dici2b*>* i2bmap = new map<int, mapi2b::dici2b*>();

		jbyteArray createbyteArray(JNIEnv * env, const char* value, int length)
		{
			if (value != null)
			{
				jbyteArray data = env->NewByteArray(length);
				env->SetByteArrayRegion(data, 0, length, (const jbyte*) value);
				return data;
			}
			else
			{
				jbyteArray data = env->NewByteArray(0);
				return data;
			}
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2BMap
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_create(JNIEnv *, jclass, jint index, jint mode)
		{
			maplock.wrlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				cout << "i2bmap exists,index:" << index << endl;
			}
			else
			{
				(*i2bmap)[index] = new mapi2b::dici2b(mode);
				cout << "create i2bmap:" << index << endl;
			}
			maplock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2BMap
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_destroy(JNIEnv *, jclass, jint index)
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
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_put(JNIEnv * env, jclass, jint index, jlong key, jbyteArray value)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{

				int length = env->GetArrayLength(value);
				jboolean copy1 = false;
				signed char* ch = env->GetByteArrayElements(value, &copy1);
				int size = (*i2bmap)[index]->insert(key, ch, length);
				env->ReleaseByteArrayElements(value, ch, 0);
				maplock.unrdlock();
				return size;
			}
			maplock.unrdlock();
			return 0;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2BMap
		 * Method:    get
		 * Signature: (IJ)[B
		 */
		JNIEXPORT jbyteArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_get(JNIEnv * env, jclass, jint index, jlong key)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int vlength = 0;
				char* result = (*i2bmap)[index]->get(key, vlength);
				maplock.unrdlock();
				jbyteArray data = createbyteArray(env, result, vlength);
				if (result != null)
				{
					delete[] result;
				}
				return data;
			}
			maplock.unrdlock();
			return createbyteArray(env, null, 0);
		}

		JNIEXPORT jbyteArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_gets(JNIEnv * env, jclass, jint index, jlongArray array)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				jboolean copy = false;
				jlong* elms = env->GetLongArrayElements(array, &copy);
				int length = env->GetArrayLength(array);
				qstardb::charwriter writer(length * 128);
				int hits = (*i2bmap)[index]->get((mapi2b::int64 *) elms, length, writer);
				env->ReleaseLongArrayElements(array, elms, 0);
				int datalength = writer.size();
				jbyteArray data = env->NewByteArray(datalength + 4);
				writer.writeInt(hits);
				env->SetByteArrayRegion(data, 0, 4, (const jbyte*) (writer.buffer + datalength));
				env->SetByteArrayRegion(data, 4, datalength, (const jbyte*) writer.buffer);
				maplock.unrdlock();
				return data;
			}
			else
			{
				maplock.unrdlock();
				return env->NewByteArray(4);
			}
		}
		JNIEXPORT int JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_delhalf(JNIEnv * env, jclass, jint index)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int size = (*i2bmap)[index]->delhalf();
				maplock.unrdlock();
				return size;
			}

			maplock.unrdlock();
			return 0;
		}
		JNIEXPORT int JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_size(JNIEnv * env, jclass, jint index)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int size = (*i2bmap)[index]->docszie();
				maplock.unrdlock();
				return size;
			}

			maplock.unrdlock();
			return 0;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_I2BMap
		 * Method:    remove
		 * Signature: (IJ)[B
		 */
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_remove(JNIEnv * env, jclass, jint index, jlong key)
		{

			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				int size = (*i2bmap)[index]->remove(key);
				maplock.unrdlock();
				return size;
			}
			maplock.unrdlock();
			return 0;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_load(JNIEnv * env, jclass, jint index, jstring file)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*i2bmap)[index]->readfile(filename);
				env->ReleaseStringUTFChars(file, ch);
				maplock.unrdlock();
				return true;
			}
			maplock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024I2BMap_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			maplock.rdlock();
			if (i2bmap->find(index) != i2bmap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*i2bmap)[index]->writefile(filename);
				env->ReleaseStringUTFChars(file, ch);
				maplock.unrdlock();
				return true;
			}
			maplock.unrdlock();
			return false;
		}

	}
#ifdef __cplusplus
}
#endif
#endif
