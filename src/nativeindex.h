/*
 * nativeindex.h
 *
 *  Created on: 2017年5月11日
 *      Author: jkuang
 */
#include <jni.h>
#include "jni.h"
#include "reversebase.h"
#ifndef _Included_org_jkuang_qstardb_Native_RIndex
#define _Included_org_jkuang_qstardb_Native_RIndex
#ifdef __cplusplus
extern "C"
{
#endif
	namespace qstardb
	{
		static rwsyslock reverseRWlock;
		static map<int, reverse::rmindex*>* indexs = new map<int, reverse::rmindex*>();
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_create(JNIEnv *, jclass, jint index, jboolean ignoreCase, jboolean compress,
				jbyteArray bytes)
		{
			reverseRWlock.wrlock();
			if (indexs->find(index) != indexs->end())
			{
				reverseRWlock.unwrlock();

				cout << "index exists,index:" << index << endl;
				return 0;
			}
			else
			{
				(*indexs)[index] = new reverse::rmindex(ignoreCase, compress);
				reverseRWlock.unwrlock();
				cout << "create index:" << index << endl;
				return 1;
			}
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_destroy(JNIEnv *, jclass, jint index)
		{
			reverseRWlock.wrlock();
			if (indexs->find(index) != indexs->end())
			{
				delete (*indexs)[index];
				indexs->erase(index);
				reverseRWlock.unwrlock();
				cout << "destroy index:" << index << endl;
				return 1;
			}
			reverseRWlock.unwrlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_load(JNIEnv *env, jclass, jint index, jstring file)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				cout << "load file:" << index << endl;
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*indexs)[index]->load(filename);
				env->ReleaseStringUTFChars(file, ch);
			}
			reverseRWlock.unrdlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				cout << "dump index:" << index << endl;
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*indexs)[index]->dump(filename);
				env->ReleaseStringUTFChars(file, ch);
			}
			reverseRWlock.unrdlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_addDoc(JNIEnv * env, jclass, jint index, jlong key, jlong sort, jobjectArray terms)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				vector<string> vl;
				jint length = env->GetArrayLength(terms);
				for (int i = 0; i < length; i++)
				{
					jstring obj = (jstring) env->GetObjectArrayElement(terms, i);
					jboolean copy = false;
					const char *ch = env->GetStringUTFChars(obj, &copy);
					string temp(ch);
					vl.push_back(temp);
					env->ReleaseStringUTFChars(obj, ch);
				}
				(*indexs)[index]->add(key, sort, vl);
				reverseRWlock.unrdlock();
				return 1;
			}
			reverseRWlock.unrdlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_delDoc(JNIEnv * env, jclass, jint index, jlong key)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				(*indexs)[index]->remove(key);
				reverseRWlock.unrdlock();
				return 1;
			}
			reverseRWlock.unrdlock();
			return 0;
		}

		jlongArray createlongArray(JNIEnv * env, vector<int64>& result, int64 offset)
		{
			jlongArray ids = env->NewLongArray(result.size() + 1);
			env->SetLongArrayRegion(ids, 0, 1, (const jlong*) &offset);
			for (size_t i = 0; i < result.size(); i++)
			{
				int64 value = result.at(i);
				env->SetLongArrayRegion(ids, i + 1, 1, (const jlong*) &value);
			}
			return ids;
		}

		JNIEXPORT jlongArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_queryByKeys(JNIEnv * env, jclass, jint index, jlongArray array)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				searchstats stat(0, 0, 0);
				jboolean copy = false;
				jlong* elms = env->GetLongArrayElements(array, &copy);
				int length = env->GetArrayLength(array);
				vector<int64> keys;
				for (int i = 0; i < length; i++)
				{
					keys.push_back(elms[i]);
				}
				if (copy)
				{
					env->ReleaseLongArrayElements(array, elms, 0);
				}
				(*indexs)[index]->query(keys, stat);
				reverseRWlock.unrdlock();
				return createlongArray(env, stat.result(), stat.getoffset());

			}
			reverseRWlock.unrdlock();
			vector<int64> result;
			return createlongArray(env, result, 0);

		}

		JNIEXPORT jlongArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RIndex_query(JNIEnv * env, jclass, jint index, jstring jsyntax, jlong _s_sort_,
				jlong _e_sort_, jint start, jint rows, jint _count, jboolean desc)
		{
			reverseRWlock.rdlock();
			if (indexs->find(index) != indexs->end())
			{
				searchstats stat(start, rows, _count);
				if (env->GetStringLength(jsyntax) == 0)
				{
					string syntax("_all:*");
					(*indexs)[index]->query(syntax, _s_sort_, _e_sort_, desc, stat);
				}
				else
				{
					jboolean copy = false;
					const char* ch = env->GetStringUTFChars(jsyntax, &copy);
					string syntax(ch);
					env->ReleaseStringUTFChars(jsyntax, ch);
					(*indexs)[index]->query(syntax, _s_sort_, _e_sort_, desc, stat);
				}
				reverseRWlock.unrdlock();
				return createlongArray(env, stat.result(), stat.getoffset());
			}
			reverseRWlock.unrdlock();
			vector<int64> result;
			return createlongArray(env, result, 0);

		}
	}
#ifdef __cplusplus
}
#endif
#endif
