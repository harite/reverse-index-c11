/*
 * nativeintset.h
 *
 *  Created on: 2015年3月11日
 *      Author: jkuang
 */
#ifndef NATIVESET_H_
#define NATIVESET_H_
#include "dichashset.h"
#ifdef __cplusplus
extern "C"
{
#endif
	namespace dicintset
	{

		static qstardb::rwsyslock rwlock;

		static map<int, setint::keyset<int>*>* int32sets = new map<int, setint::keyset<int>*>();

		static map<int, setint::keyset<setint::int64>*>* int64sets = new map<int, setint::keyset<setint::int64>*>();
		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_create(JNIEnv *, jclass, jint index, jint mode)
		{
			rwlock.wrlock();
			if (int32sets->find(index) != int32sets->end())
			{
				cout << "int32set exists,index:" << index << endl;
			}
			else
			{
				(*int32sets)[index] = new setint::keyset<int>(mode);
				cout << "create int32set:" << index << endl;
			}
			rwlock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_destroy(JNIEnv *, jclass, jint index)
		{
			rwlock.wrlock();
			if (int32sets->find(index) != int32sets->end())
			{
				delete (*int32sets)[index];
				int32sets->erase(index);
				cout << "destroy int32set ,index:" << index << endl;
			}
			else
			{
				cout << "int32set not exists:" << index << endl;
			}
			rwlock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    add
		 * Signature: (II)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_add(JNIEnv *, jclass, jint index, jint key)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				(*int32sets)[index]->add(key);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    remove
		 * Signature: (II)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_remove(JNIEnv *, jclass, jint index, jint key)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				(*int32sets)[index]->remove(key);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    contains
		 * Signature: (II)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_contains(JNIEnv *, jclass, jint index, jint key)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				bool result = (*int32sets)[index]->contains(key);
				rwlock.unrdlock();
				return result;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int32Set
		 * Method:    size
		 * Signature: (I)I
		 */
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_size(JNIEnv *, jclass, jint index)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				int result = (*int32sets)[index]->keysize();
				rwlock.unrdlock();
				return result;
			}
			rwlock.unrdlock();
			return false;
		}
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_load(JNIEnv * env, jclass, jint index, jstring file)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*int32sets)[index]->readfile(filename);
				env->ReleaseStringUTFChars(file, ch);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int32Set_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			rwlock.rdlock();
			if (int32sets->find(index) != int32sets->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*int32sets)[index]->writefile(filename);
				env->ReleaseStringUTFChars(file, ch);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_create(JNIEnv *, jclass, jint index, jint mode)
		{
			rwlock.wrlock();
			if (int64sets->find(index) != int64sets->end())
			{
				cout << "int64set exists,index:" << index << endl;
			}
			else
			{
				(*int64sets)[index] = new setint::keyset<setint::int64>(mode);
				cout << "create int64set:" << index << endl;
			}
			rwlock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_destroy(JNIEnv *, jclass, jint index)
		{

			rwlock.wrlock();
			if (int64sets->find(index) != int64sets->end())
			{
				delete (*int64sets)[index];
				int64sets->erase(index);
				cout << "destroy int32set ,index:" << index << endl;
			}
			else
			{
				cout << "int32set not exists:" << index << endl;
			}
			rwlock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    add
		 * Signature: (IJ)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_add(JNIEnv *, jclass, jint index, jlong key)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				(*int64sets)[index]->add(key);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    remove
		 * Signature: (IJ)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_remove(JNIEnv *, jclass, jint index, jlong key)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				(*int64sets)[index]->remove(key);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    contains
		 * Signature: (IJ)Z
		 */
		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_contains(JNIEnv *, jclass, jint index, jlong key)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				bool result = (*int64sets)[index]->contains(key);
				rwlock.unrdlock();
				return result;
			}
			rwlock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_load(JNIEnv * env, jclass, jint index, jstring file)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*int64sets)[index]->readfile(filename);
				env->ReleaseStringUTFChars(file, ch);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*int64sets)[index]->writefile(filename);
				env->ReleaseStringUTFChars(file, ch);
				rwlock.unrdlock();
				return true;
			}
			rwlock.unrdlock();
			return false;
		}
		/*
		 * Class:     org_jkuang_qstardb_NativeSet_Int64Set
		 * Method:    size
		 * Signature: (I)I
		 */
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024Int64Set_size(JNIEnv *, jclass, jint index)
		{
			rwlock.rdlock();
			if (int64sets->find(index) != int64sets->end())
			{
				int result = (*int64sets)[index]->keysize();
				rwlock.unrdlock();
				return result;
			}
			rwlock.unrdlock();
			return false;
		}
	}
#ifdef __cplusplus
}
#endif
#endif /* NATIVESET_H_ */
