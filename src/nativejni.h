#pragma once
#include <jni.h>
#include <vector>
#include "filestream.h"
#ifndef _NATIVEJNI_H
#define _NATIVEJNI_H
using namespace qstardb;
jbyteArray createJByteArray(JNIEnv * env, const char* value, int length)
{
	if (value != nullptr)
	{
		jbyteArray data = env->NewByteArray(length);
		env->SetByteArrayRegion(data, 0, length, (const jbyte*)value);
		return data;
	}
	else
	{
		jbyteArray data = env->NewByteArray(0);
		return data;
	}
}

jbyteArray createJBytes(JNIEnv * env, qstardb::charwriter& writer)
{
	jbyteArray data = env->NewByteArray(writer.size());
	env->SetByteArrayRegion(data, 0, writer.size(), (const jbyte*)writer.buffer);
	return data;
}

jlongArray createJLongArray(JNIEnv * env, vector<int64>& result, int64 offset)
{
	jlongArray ids = env->NewLongArray(result.size() + 1);
	env->SetLongArrayRegion(ids, 0, 1, (const jlong*)&offset);
	for (size_t i = 0; i < result.size(); i++)
	{
		int64 value = result.at(i);
		env->SetLongArrayRegion(ids, i + 1, 1, (const jlong*)&value);
	}
	return ids;
}

jstring charToJString(JNIEnv* env, const char* value, int len)
{
	if (value != nullptr&&len > 0) {
		jclass strClass = (env)->FindClass("Ljava/lang/String;");
		jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
		jbyteArray bytes = (env)->NewByteArray(len);
		(env)->SetByteArrayRegion(bytes, 0, len, (jbyte*)value);
		jstring encoding = (env)->NewStringUTF("UTF8");
		return (jstring)(env)->NewObject(strClass, ctorID, bytes, encoding);
	}
	else {
		return (env)->NewStringUTF("");
	}
	
}
#endif