/*
 * nativeindex.h
 *
 *  Created on: 2017年5月11日
 *      Author: jkuang
 */
#include "nativejni.h"
#include "reversebase.h"
#ifndef _Included_org_jkuang_qstardb_Native_RMDB
#define _Included_org_jkuang_qstardb_Native_RMDB
#ifdef __cplusplus
extern "C"
{
#endif
	namespace qstardb
	{
		static rwsyslock rmdblock;
		static map<int, reverse::rmdb*>* rmdbs = new map<int, reverse::rmdb*>();
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_create(JNIEnv *, jclass, jint index, jboolean ignoreCase, jboolean compress,
			jbyteArray bytes)
		{
			rmdblock.wrlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				rmdblock.unwrlock();

				cout << "index exists,index:" << index << endl;
				return 0;
			}
			else
			{
				(*rmdbs)[index] = new reverse::rmdb(ignoreCase, compress);
				rmdblock.unwrlock();
				cout << "create index:" << index << endl;
				return 1;
			}
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_destroy(JNIEnv *, jclass, jint index)
		{
			rmdblock.wrlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				delete (*rmdbs)[index];
				rmdbs->erase(index);
				rmdblock.unwrlock();
				cout << "destroy index:" << index << endl;
				return 1;
			}
			rmdblock.unwrlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_addDoc(JNIEnv * env, jclass, jint index, jlong key, jlong sort, jobjectArray terms, jbyteArray data)
		{
			rmdblock.rdlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				vector<string> vl;
				jint length = env->GetArrayLength(terms);
				for (int i = 0; i < length; i++)
				{
					jstring obj = (jstring)env->GetObjectArrayElement(terms, i);
					jboolean copy = false;
					const char *ch = env->GetStringUTFChars(obj, &copy);
					string temp(ch);
					vl.push_back(temp);
					env->ReleaseStringUTFChars(obj, ch);
				}

				int datalength = env->GetArrayLength(data);
				jboolean copy1 = false;
				signed char* ch = env->GetByteArrayElements(data, &copy1);
				(*rmdbs)[index]->add(key, sort, vl,(const char*) ch, datalength);
				//释放data
				env->ReleaseByteArrayElements(data, ch, 0);

				rmdblock.unrdlock();
				return 1;
			}
			rmdblock.unrdlock();
			return 0;
		}

		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_delDoc(JNIEnv * env, jclass, jint index, jlong key)
		{
			rmdblock.rdlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				(*rmdbs)[index]->remove(key);
				rmdblock.unrdlock();
				return 1;
			}
			rmdblock.unrdlock();
			return 0;
		}




		JNIEXPORT jbyteArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_queryByKeys(JNIEnv * env, jclass, jint index, jlongArray array)
		{
			rmdblock.rdlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				
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
				charwriter writer(1024*16);
				(*rmdbs)[index]->query(keys, writer);
				rmdblock.unrdlock();
				return createJBytes(env, writer);

			}
			rmdblock.unrdlock();
			return env->NewByteArray(0);

		}

		JNIEXPORT jbyteArray JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024RMDB_query(JNIEnv * env, jclass, jint index, jstring jsyntax, jlong _s_sort_,jlong _e_sort_, jint start, jint rows, jint _count, jboolean desc)
		{
			rmdblock.rdlock();
			if (rmdbs->find(index) != rmdbs->end())
			{
				charwriter writer(1024 * 16);
				searchstats stat(start, rows, _count);
				if (env->GetStringLength(jsyntax) == 0)
				{
					string syntax("_all:*");
					(*rmdbs)[index]->query(syntax, _s_sort_, _e_sort_, desc, stat,writer);
				}
				else
				{
					jboolean copy = false;
					const char* ch = env->GetStringUTFChars(jsyntax, &copy);
					string syntax(ch);
					env->ReleaseStringUTFChars(jsyntax, ch);
					(*rmdbs)[index]->query(syntax, _s_sort_, _e_sort_, desc, stat, writer);
				}
				rmdblock.unrdlock();
				return createJBytes(env, writer);
			}
			rmdblock.unrdlock();
			return env->NewByteArray(0);

		}
	}
#ifdef __cplusplus
}
#endif
#endif
