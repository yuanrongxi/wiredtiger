#include "wt_internal.h"

/*动态加载so文件*/
int __wt_dlopen(WT_SESSION_IMPL *session, const char *path, WT_DLH **dlhp)
{
	WT_DECL_RET;
	WT_DLH *dlh;

	//WT_RET(__wt_calloc(session, (size_t)1, sizeof(WT_DLH), (void*)(&dlh)));
	WT_RET(__wt_calloc_one(session, (&dlh)));
	WT_ERR(__wt_strdup(session, path, &(dlh->name)));

	if ((dlh->handle = dlopen(path, RTLD_LAZY)) == NULL)
		WT_ERR_MSG(session, __wt_errno(), "dlopen(%s): %s", path, dlerror());

	*dlhp = dlh;

err:
	__wt_free(session, dlh->name);
	__wt_free(session, dlh);

	return ret;
}

/*从动态加载的SO中查找函数符号*/
int __wt_dlsym(WT_SESSION_IMPL *session, WT_DLH *dlh, const char *name, int fail, void *sym_ret)
{
	void *sym;

	*(void **)sym_ret = NULL;
	if ((sym = dlsym(dlh->handle, name)) == NULL) {
		if (fail)
			WT_RET_MSG(session, __wt_errno(), "dlsym(%s in %s): %s", name, dlh->name, dlerror());

		return 0;
	}

	*(void **)sym_ret = sym;
	return 0;
}

int __wt_dlclose(WT_SESSION_IMPL *session, WT_DLH *dlh)
{
	WT_DECL_RET;

	if (dlclose(dlh->handle) != 0) {
		ret = __wt_errno();
		__wt_err(session, ret, "dlclose: %s", dlerror());
	}

	__wt_free(session, dlh->name);
	__wt_free(session, dlh);
}

