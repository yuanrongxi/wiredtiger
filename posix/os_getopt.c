#include "wt_internal.h"

extern int __wt_opterr, __wt_optind, __wt_optopt, __wt_optreset;
int __wt_opterr = 1, __wt_optind = 1, __wt_optopt, __wt_optreset;

extern char* __wt_optarg;
char* __wt_optarg;


#define	BADCH	(int)'?'
#define	BADARG	(int)':'
#define	EMSG	""

/*解析main函数的 argc/argv 命令参数*/
int __wt_getopt(const char *progname, int nargc, char * const *nargv, const char *ostr)
{
	static const char *place = EMSG;	/* option letter processing */
	const char *oli;					/* option letter list index */

	/*检查命令参数的合法性，第一个参数必须是执行命令，不能是-带头的命令选项*/
	if (__wt_optreset || *place == 0) {	/* update scanning pointer */
		__wt_optreset = 0;
		place = nargv[__wt_optind];
		if (__wt_optind >= nargc || *place++ != '-') {
			/* Argument is absent or is not an option */
			place = EMSG;
			return (-1);
		}

		/*带有-参数的后面，必须有命令选项名*/
		__wt_optopt = *place++;
		if (__wt_optopt == '-' && *place == 0) {
			/* "--" => end of options */
			++__wt_optind;
			place = EMSG;
			return (-1);
		}

		if (__wt_optopt == 0) {
			/* Solitary '-', treat as a '-' option
			   if the program (eg su) is looking for it. */
			place = EMSG;
			if (strchr(ostr, '-') == NULL)
				return (-1);
			__wt_optopt = '-';
		}
	} 
	else
		__wt_optopt = *place++;

	/* See if option letter is one the caller wanted... */
	if (__wt_optopt == ':' || (oli = strchr(ostr, __wt_optopt)) == NULL) {
		if (*place == 0)
			++__wt_optind;
		if (__wt_opterr && *ostr != ':')
			fprintf(stderr, "%s: illegal option -- %c\n", progname, __wt_optopt);

		return BADCH;
	}

	/* Does this option need an argument? */
	if (oli[1] != ':') {
		/* don't need argument */
		__wt_optarg = NULL;
		if (*place == 0)
			++__wt_optind;
	} 
	else {
		/* Option-argument is either the rest of this argument or the
		   entire next argument. */
		if (*place)
			__wt_optarg = (char *)place;
		else if (nargc > ++__wt_optind)
			__wt_optarg = nargv[__wt_optind];
		else {
			/* option-argument absent */
			place = EMSG;
			if (*ostr == ':')
				return (BADARG);
			if (__wt_opterr)
				(void)fprintf(stderr,
				    "%s: option requires an argument -- %c\n",
				    progname, __wt_optopt);
			return (BADCH);
		}

		place = EMSG;
		++__wt_optind;
	}

	return __wt_optopt;			/* return option letter */
}


