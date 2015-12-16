/***********************************************************************************************
定义GCC编译器的特性函数和宏
***********************************************************************************************/

#define WT_SIZET_FMT		"zu"

/*定义gcc一个字节对齐特性*/
#define WT_COMPILER_TYPE_ALIGN(x)			__attribute__((align(x)))

/*定义一个__packed__对齐的struct宏对,其实就是不对齐*/
#define	WT_PACKED_STRUCT_BEGIN(name) struct __attribute__ ((__packed__)) name {
#define	WT_PACKED_STRUCT_END };

#define WT_GCC_FUNC_ATTRIBUTE(x)
#define WT_GCC_FUNC_DECL_ATTRIBUTE(x) __attribute__(x)

/*原子操作封装, WT_STATIC_ASSERT只是用来做编译检查用的*/

#define __WT_ATOMIC_ADD(v, val, n)			(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_add_and_fetch(&(v), val))

#define __WT_ATOMIC_FETCH_ADD(v, val, n)	(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_fetch_and_add(&(v), val))

/*如果v == old,将v赋值为new,这个宏是一个值是一个bool类型的值,赋值成功会是一个TRUE值*/
#define __WT_ATOMIC_CAS(v, old, new, n)		(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_val_compare_and_swap(&(v), old, new) == (old))
/*如果v == old,将v赋值为new*/
#define	__WT_ATOMIC_CAS_VAL(v, old, new, n)	(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_val_compare_and_swap(&(v), old, new))
/*将val赋值给v,并返回v设置之前的值？*/
#define __WT_ATOMIC_STORE(v, val, n)		(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_lock_test_and_set(&(v), val))

#define __WT_ATOMIC_SUB(v, val, n)			(WT_STATIC_ASSERT(sizeof(v) == (n)), __sync_sub_and_fetch(&(v), val))

/*对各个整型数的原子操作宏*/
/*int8_t*/
#define	WT_ATOMIC_ADD1(v, val)				__WT_ATOMIC_ADD(v, val, 1)
#define	WT_ATOMIC_FETCH_ADD1(v, val)		__WT_ATOMIC_FETCH_ADD(v, val, 1)
#define	WT_ATOMIC_CAS1(v, old, new)			__WT_ATOMIC_CAS(v, old, new, 1)
#define	WT_ATOMIC_CAS_VAL1(v, old, new)		__WT_ATOMIC_CAS_VAL(v, old, new, 1)
#define	WT_ATOMIC_STORE1(v, val)			__WT_ATOMIC_STORE(v, val, 1)
#define	WT_ATOMIC_SUB1(v, val)				__WT_ATOMIC_SUB(v, val, 1)

/*int16_t*/
#define	WT_ATOMIC_ADD2(v, val)				__WT_ATOMIC_ADD(v, val, 2)
#define	WT_ATOMIC_FETCH_ADD2(v, val)		__WT_ATOMIC_FETCH_ADD(v, val, 2)
#define	WT_ATOMIC_CAS2(v, old, new)			__WT_ATOMIC_CAS(v, old, new, 2)
#define	WT_ATOMIC_CAS_VAL2(v, old, new)		__WT_ATOMIC_CAS_VAL(v, old, new, 2)
#define	WT_ATOMIC_STORE2(v, val)			__WT_ATOMIC_STORE(v, val, 2)
#define	WT_ATOMIC_SUB2(v, val)				__WT_ATOMIC_SUB(v, val, 2)

/*int32_t*/
#define	WT_ATOMIC_ADD4(v, val)				__WT_ATOMIC_ADD(v, val, 4)
#define	WT_ATOMIC_FETCH_ADD4(v, val)		__WT_ATOMIC_FETCH_ADD(v, val, 4)
#define	WT_ATOMIC_CAS4(v, old, new)			__WT_ATOMIC_CAS(v, old, new, 4)
#define	WT_ATOMIC_CAS_VAL4(v, old, new)		__WT_ATOMIC_CAS_VAL(v, old, new, 4)
#define	WT_ATOMIC_STORE4(v, val)			__WT_ATOMIC_STORE(v, val, 4)
#define	WT_ATOMIC_SUB4(v, val)				__WT_ATOMIC_SUB(v, val, 4)

/*int64_t*/
#define	WT_ATOMIC_ADD8(v, val)				__WT_ATOMIC_ADD(v, val, 8)
#define	WT_ATOMIC_FETCH_ADD8(v, val)		__WT_ATOMIC_FETCH_ADD(v, val, 8)
#define	WT_ATOMIC_CAS8(v, old, new)			__WT_ATOMIC_CAS(v, old, new, 8)
#define	WT_ATOMIC_CAS_VAL8(v, old, new)		__WT_ATOMIC_CAS_VAL(v, old, new, 8)
#define	WT_ATOMIC_STORE8(v, val)			__WT_ATOMIC_STORE(v, val, 8)
#define	WT_ATOMIC_SUB8(v, val)				__WT_ATOMIC_SUB(v, val, 8)

/*内存屏障宏定义*/

/*编译屏障,防止编译器乱序优化指令*/
#define WT_BARRIER()						__asm__ volatile("" ::: "memory")

/*用汇编指令PAUSE来防止过度消耗CPU,尤其是在空循环体中，加入PAUSE指令能防止CPU退出循环体时太多的异常指令消耗*/
#define WT_PAUSE()							__asm__ volatile("pause\n" ::: "memory")

/*定义CPU屏障*/
#if defined(x86_64) || defined(__x86_64__) /*64位CPU*/
#define WT_FULL_BARRIER() do{ __asm__ volatile("mfence" ::: "memory");}while(0)
#define WT_READ_BARRIER() do{ __asm__ volatile("lfence" ::: "memory");}while(0)
#define WT_WRITE_BARRIER() do{ __asm__ volatile("sfence" ::: "memory");}while(0)
#elif defined(i386) || defined(__i386__) /*32为CPU*/
#define WT_FULL_BARRIER() do{__asm__ volatile ("lock; addl $0, 0(%%esp)" ::: "memory");}while(0)
#define WT_READ_BARRIER() WT_FULL_BARRIER()
#define WT_WRITE_BARRIER() WT_FULL_BARRIER()
#else /*其他类CPU，直接提示编译错误*/
#error "No write barrier implementation for this hardware"
#endif
/**********************************************************************************************/




