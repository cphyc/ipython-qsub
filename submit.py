from IPython.core.magic import Magics, magics_class, cell_magic

import argparse
import tempfile
from string import Template
import os
import sh
from os.path import join as J

try:
    import cPickle as pickle
except:
    import pickle as pickle

# Templates
pickleLoad = Template('''try:
    import cPickle as pickle
except:
    import pickle as pickle
with open('$dump', 'rb') as f:
    _pickleLoaded = pickle.load(f)

    for el in _pickleLoaded:
        globals()[el] = _pickleLoaded[el]
''')

def genPickleDump(outvar, dumpfile):
    pickleDump = Template('''with open('$dump', 'wb') as f:
        pickle.dump($outvar, f)''')
    if outvar:
        return pickleDump.substitute(outvar=outvar, dump=dumpfile)
    else:
        return ''


def gen_qsub_script(script, name='qsub_magic', nodes=1, ppn=1, walltime='01:00:00',
                    mailto='cadiou@iap.fr', path=os.getcwd(), pre=[], post=[], logfile=''):
    ''' Generate a template to be runned using qsub.'''
    tplate = Template('\n'.join(
        ['#!/bin/sh',
         '#PBS -S /bin/sh',
         '#PBS -N $name',
         '#PBS -j oe',
         '#PBS -l nodes=$nodes:ppn=$ppn,walltime=$walltime',
         '#PBS -M $mailto',
         '#PBS -m abe',
         'cd $path',
         'ulimit -s unlimited'] +
         pre +
         [ 'python $script > $logfile' ] +
         post +
         ['']))
    qsub_script = tplate.substitute(name=name, nodes=nodes, ppn=ppn,
                                    walltime=walltime, mailto=mailto,
                                    path=path, script=script, logfile=logfile)

    return qsub_script

def gen_python_script(pre, body, post):
    return '\n'.join([pre, '', body, '', post])

@magics_class
class QsubMagics(Magics):
    '''Execute the content of the cell remotely using qsub

    Usage: %%qsub var1 var2 var3 [--out tmpfile]

    --out  name of the output variable
    --dry  only do a dry run
    --pre  lines to call before the cell
    --post lines to call after the cell'''

    parser = argparse.ArgumentParser(description='Run the cell on a cluster.')
    parser.add_argument('vars', nargs='*', help='The variables to pass.')
    parser.add_argument('--out', type=str,
                        help='The name of output variable', default='')
    parser.add_argument('--dry', action='store_true', default=False, help='Do a dry run.')
    parser.add_argument('--pre', nargs='*',
                        help='Extra lines to execute before calling the python script.',
                        default=[])
    parser.add_argument('--post', nargs='*',
                        help='Extra lines to execute after calling the python script.',
                        default=[])

    def __init__(self, shell):
        super(QsubMagics, self).__init__(shell=shell)
        self.ip = get_ipython()

    @cell_magic
    def qsub(self, line, cell):
        args = self.parser.parse_args(line.split(' '))

        ip = get_ipython()
        newNs = dict()

        for var in args.vars:
            if var in self.ip.user_ns:
                newNs[var] = self.ip.user_ns[var]
            elif var == '':
                continue
            else:
                raise Exception('Variable "%s" not in user namespace' % var)
                return

        # create tmpdir
        tmpdir = tempfile.mkdtemp()

        dump_in_n = J(tmpdir, 'dump_in')
        python_file_n = J(tmpdir, 'script.py')
        bash_file_n = J(tmpdir, 'script.sh')
        dump_out_n = J(tmpdir, 'dump_out')
        logfile = J(tmpdir, 'log')
        # get tmp files for io
        dump_in = open(dump_in_n, 'wb')
        python_file = open(python_file_n, 'w')
        bash_file = open(bash_file_n, 'w')

        # generate the scripts
        qsub_script = gen_qsub_script(python_file_n, pre=args.pre, post=args.post,
                                      logfile=logfile)
        python_script = gen_python_script(
            pickleLoad.substitute(dump=dump_in_n),
            cell,
            genPickleDump(dumpfile=dump_out_n,
                          outvar=args.out))
        if not args.dry:
            # print(python_file, bash_file)
            # write the python and bash files
            python_file.write(python_script)
            bash_file.write(qsub_script)

            python_file.flush()
            bash_file.flush()

            # dump the input variables
            pickle.dump(newNs, dump_in)
            dump_in.flush()

            # execute process
            p = sh.sh(bash_file_n)

            if args.out != '':
                with open(dump_out_n, 'rb') as dump_out:
                    # load the result
                    res = pickle.load(dump_out)

                    if type(res) == dict:
                        # import into local namespace
                        for key in res:
                            self.ip.user_ns[key] = res[key]
                    else:
                        # import into local namespace using outname
                        self.ip.user_ns[args.out] = res

        return



def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(QsubMagics)

load_ipython_extension(get_ipython())
