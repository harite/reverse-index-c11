/*
 * nativedics2s.h
 *
 *  Created on: 2015年3月11日
 *      Author: jkuang
 */
#include <jni.h>
#include "dicmaps2s.h"
#include <map>
#ifndef _Included_org_jkuang_qstardb_Native_S2SMap
#define _Included_org_jkuang_qstardb_Native_S2SMap
#ifdef __cplusplus
extern "C"
{
#endif
	namespace dics2smap
	{

		static qstardb::rwsyslock maplock;

		static map<int, maps2s::dics2s*>* dicmaps = new map<int, maps2s::dics2s*>();

		jstring charTojstring(JNIEnv* env, const char* value, int len)
		{
			jclass strClass = (env)->FindClass("Ljava/lang/String;");
			jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
			jbyteArray bytes = (env)->NewByteArray(len);
			(env)->SetByteArrayRegion(bytes, 0, len, (jbyte*) value);
			jstring encoding = (env)->NewStringUTF("UTF8");
			return (jstring) (env)->NewObject(strClass, ctorID, bytes, encoding);
		}
		/*
		 * Class:     org_jkuang_qstardb_Native_DicMap
		 * Method:    create
		 * Signature: (II)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_create(JNIEnv *, jclass, jint index, jint mode)
		{
			maplock.wrlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				cout << "intmap exists,index:" << index << endl;
			}
			else
			{
				(*dicmaps)[index] = new maps2s::dics2s(mode);
				cout << "create intmap:" << index << endl;
			}
			maplock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_DicMap
		 * Method:    destroy
		 * Signature: (I)V
		 */
		JNIEXPORT void JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_destroy(JNIEnv *, jclass, jint index)
		{
			maplock.wrlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				delete (*dicmaps)[index];
				dicmaps->erase(index);
				cout << "destroy intmap ,index:" << index << endl;
			}
			else
			{
				cout << "intmap not exists:" << index << endl;
			}
			maplock.unwrlock();
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_DicMap
		 * Method:    put
		 * Signature: (ILjava/lang/String;Ljava/lang/String;)I
		 */
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_put(JNIEnv *env, jclass, jint index, jstring key, jstring value)
		{
			maplock.rdlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				jboolean copy = false;
				short klen = env->GetStringUTFLength(key);
				int vlen = env->GetStringUTFLength(value);
				const char* ckey = env->GetStringUTFChars(key, &copy);
				const char* cvalue = env->GetStringUTFChars(value, &copy);
				int size = (*dicmaps)[index]->insert(ckey, klen, cvalue, vlen);
				env->ReleaseStringUTFChars(key, ckey);
				env->ReleaseStringUTFChars(value, cvalue);
				maplock.unrdlock();
				return size;

			}
			maplock.unrdlock();
			return 0;
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_DicMap
		 * Method:    get
		 * Signature: (ILjava/lang/String;)Ljava/lang/String;
		 */
		JNIEXPORT jstring JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_get(JNIEnv * env, jclass, jint index, jstring key)
		{
			maplock.rdlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				int vlen = 0;
				jboolean copy = false;
				short klen = env->GetStringUTFLength(key);
				const char* ch = env->GetStringUTFChars(key, &copy);
				const char* value = (*dicmaps)[index]->get(ch, klen, vlen);
				env->ReleaseStringUTFChars(key, ch);
				if (value != null)
				{
					jstring jvalue = charTojstring(env, value, vlen);
					delete value;
					maplock.unrdlock();
					return jvalue;
				}
			}
			maplock.unrdlock();
			return env->NewStringUTF("not found");
		}

		/*
		 * Class:     org_jkuang_qstardb_Native_DicMap
		 * Method:    remove
		 * Signature: (ILjava/lang/String;)Z
		 */
		JNIEXPORT jint JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_remove(JNIEnv * env, jclass, jint index, jstring key)
		{
			maplock.rdlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				jboolean copy = false;
				short klen = env->GetStringUTFLength(key);
				const char* ckey = env->GetStringUTFChars(key, &copy);
				int size = (*dicmaps)[index]->remove(ckey, klen);
				env->ReleaseStringUTFChars(key, ckey);
				maplock.unrdlock();
				return size;
			}
			maplock.unrdlock();
			return 0;

		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_load(JNIEnv * env, jclass, jint index, jstring file)
		{
			maplock.rdlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*dicmaps)[index]->readfile(filename);
				env->ReleaseStringUTFChars(file, ch);
				maplock.unrdlock();
				return true;
			}
			maplock.unrdlock();
			return false;
		}

		JNIEXPORT jboolean JNICALL JNICALL Java_org_jkuang_qstar_index_jni_Native_00024S2SMap_dump(JNIEnv *env, jclass, jint index, jstring file)
		{
			maplock.rdlock();
			if (dicmaps->find(index) != dicmaps->end())
			{
				jboolean copy = false;
				const char* ch = env->GetStringUTFChars(file, &copy);
				string filename(ch);
				(*dicmaps)[index]->writefile(filename);
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
