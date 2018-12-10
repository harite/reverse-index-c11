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
#include "elasticb2i.h"
#ifndef _Included_org_jkuang_qstardb_S2I
#define _Included_org_jkuang_qstardb_S2I
#ifdef __cplusplus
extern "C"
{
#endif
	using namespace std;

	namespace seqmap
	{
		static qstardb::rwsyslock maplock;

		static map<int, b2imap*>* smap = new map<int, b2imap*>();

		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_create(JNIEnv *, jclass, jint index,jint part)
		{
			maplock.wrlock();
			if (smap->find(index) != smap->end())
			{
				cout << "smap exists,index:" << index << endl;
			}
			else
			{
				(*smap)[index] = new b2imap(part);
				cout << "create smap:" << index << endl;
			}
			maplock.unwrlock();
		}

		JNIEXPORT void JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_destroy(JNIEnv *, jclass, jint index)
		{
			maplock.wrlock();
			if (smap->find(index) != smap->end())
			{
				delete (*smap)[index];
				smap->erase(index);
				cout << "destroy smap ,index:" << index << endl;
			}
			else
			{
				cout << "smap not exists:" << index << endl;
			}
			maplock.unwrlock();

		}


		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_put(JNIEnv * env, jclass, jint index, jstring key)
		{
			maplock.rdlock();
			if (smap->find(index) != smap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(key, &copy);
				int length = env->GetStringUTFLength(key);
				int result=(*smap)[index]->add(ch, length);
				env->ReleaseStringUTFChars(key, ch);
				maplock.unrdlock();
				return result;
			}
			maplock.unrdlock();
			return 0;
		}


		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_get(JNIEnv * env, jclass, jint index, jstring key)
		{
			maplock.rdlock();
			if (smap->find(index) != smap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(key, &copy);
				string temp(ch);
				int result = (*smap)[index]->get(temp.c_str(), temp.length());
				env->ReleaseStringUTFChars(key, ch);
				maplock.unrdlock();
				return result;
			}
			maplock.unrdlock();
			return -1;
		}

		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_remove(JNIEnv * env, jclass, jint index, jstring key)
		{

			maplock.rdlock();
			if (smap->find(index) != smap->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(key, &copy);
				string temp(ch);
				bool result = (*smap)[index]->remove(temp.c_str(), temp.length());
				env->ReleaseStringUTFChars(key, ch);
				maplock.unrdlock();
				return result;
			}
			maplock.unrdlock();
			return 0;
		}

		JNIEXPORT jstring JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_find(JNIEnv * env, jclass, jint index, jint key)
		{

			maplock.rdlock();
			if (smap->find(index) != smap->end())
			{
				string str;
				if ((*smap)[index]->get(key, str)) {
					maplock.unrdlock();
					return charToJString(env, str.c_str(), str.length());
				}
			}
			maplock.unrdlock();
			return charToJString(env,nullptr,0);
		}

		JNIEXPORT jint JNICALL Java_org_jkuang_qstar_commons_jni_DicMap_maxSeq(JNIEnv * env, jclass, jint index)
		{

			maplock.rdlock();
			if (smap->find(index) != smap->end())
			{
				string str;
				int result = (*smap)[index]->curSeq();
				maplock.unrdlock();
				return result;
			}
			maplock.unrdlock();
			return 0;
		}
	}
#ifdef __cplusplus
}
#endif
#endif
