/*
 * rwsyslock.h
 *  操作系统读写锁
 *  Created on: 2015年2月11日
 *      Author: jkuang
 */
#ifndef RWSYSLOCK_H_
#define RWSYSLOCK_H_
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>  
#ifndef _WIN32
#include <unistd.h>
#include <pthread.h>
#else
#include <windows.h>  
#include <process.h> 
#endif
namespace qstardb
{
#ifndef _WIN32

	class rwsyslock
	{
		pthread_rwlock_t rwlock;
	public:
		rwsyslock()
		{
			pthread_rwlock_init(&rwlock,  nullptr);
		}
		~rwsyslock()
		{
			pthread_rwlock_destroy(&rwlock);
		}
		inline void wrlock()
		{
			pthread_rwlock_wrlock(&rwlock);
		}
		inline void unwrlock()
		{
			pthread_rwlock_unlock(&rwlock);
		}
		inline void rdlock()
		{
			pthread_rwlock_rdlock(&rwlock);
		}
		inline void unrdlock()
		{
			pthread_rwlock_unlock(&rwlock);
		}

		inline int trywrlock()
		{
			return pthread_rwlock_trywrlock(&rwlock);
		}
		inline int tryrdlock()
		{
			return pthread_rwlock_tryrdlock(&rwlock);
		}
	};
#elif (_WIN32_WINNT >= 0x0600)

class rwsyslock
{
	SRWLOCK rwlock;
public:
	rwsyslock()
	{
		InitializeSRWLock(&rwlock);
	}
	~rwsyslock()
	{

	}
	inline void wrlock()
	{
		AcquireSRWLockExclusive(&rwlock);
	}
	inline void unwrlock()
	{
		ReleaseSRWLockExclusive(&rwlock);
	}
	inline void rdlock()
	{
		AcquireSRWLockShared(&rwlock);
	}
	inline void unrdlock()
	{
		ReleaseSRWLockShared(&rwlock);
	}

	inline int trywrlock()
	{
		return TryAcquireSRWLockExclusive(&rwlock);
	}
	inline int tryrdlock()
	{
		return TryAcquireSRWLockShared(&rwlock);
	}
};
#else
class rwsyslock
{
	LPCRITICAL_SECTION lpCriticalSection;
public:
	rwsyslock()
	{
		InitializeCriticalSection(&lpCriticalSection);
	}
	~rwsyslock()
	{
		DeleteCriticalSection(&lpCriticalSection);
	}
	inline void wrlock()
	{
		EnterCriticalSection(&lpCriticalSection);
	}
	inline void unwrlock()
	{
		LeaveCriticalSection(&lpCriticalSection);
	}
	inline void rdlock()
	{
		EnterCriticalSection(&lpCriticalSection);
	}
	inline void unrdlock()
	{
		LeaveCriticalSection(&lpCriticalSection);
	}

	inline int trywrlock()
	{
		return TryEnterCriticalSection(&lpCriticalSection);
	}
	inline int tryrdlock()
	{
		return TryEnterCriticalSection(&lpCriticalSection);
	}
};
#endif
}
#endif
