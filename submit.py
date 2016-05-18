from IPython.core.magic import Magics, magics_class, cell_magic

import argparse
import tempfile
from string import Template
import os
import sh
from os.path import join as J
import time

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
pickleDump = Template('''if $doDump:
    with open('$dump', 'wb') as f:
        pickle.dump($outvar, f)
''')

def genPickleDump(outvar, dumpfile):

    return pickleDump.substitute(doDump=(outvar != ''),
                                 outvar=(outvar if outvar != '' else 'None'),
                                 dump=dumpfile)


def gen_qsub_script(script, name='qsub_magic', nodes=1, ppn=1, walltime='01:00:00',
                    mailto='cadiou@iap.fr', path=os.getcwd(), pre=[], post=[], logfile='',
                    isdonefile=''):
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
        [ 'python $script > $logfile &&',
          '    echo 0 > $isdonefile ||',
          '    echo 1 > $isdonefile'] +
        post +
        ['return 0']))
    qsub_script = tplate.substitute(name=name, nodes=nodes, ppn=ppn,
                                    walltime=walltime, mailto=mailto,
                                    path=path, script=script, logfile=logfile,
                                    isdonefile=isdonefile)

    return qsub_script

def gen_python_script(pre, body, post):
    return '\n'.join([pre, '', body, '', post])

@magics_class
class QsubMagics(Magics):
    parser = argparse.ArgumentParser(description='Execute the content of the cell remotely using qsub.',
                                     prog='%%qsub', add_help=False)
    parser.add_argument('vars', nargs='*', help='The variables to pass.')
    parser.add_argument('--dry', action='store_true', default=False, help='Do a dry run.')
    parser.add_argument('--pre', nargs='*',
                        help='Extra lines to execute before calling the python script.',
                        default=[])
    parser.add_argument('--post', nargs='*',
                        help='Extra lines to execute after calling the python script.',
                        default=[])
    parser.add_argument('--out', metavar='outvar', type=str,
                        help='Name of the output variable (only 1 is supported for now)', default='')
    parser.add_argument('--noclean', action='store_true', default=False,
                        help='Do not clean temporary directory after script execution')
    parser.add_argument('--tmpdir', default=None,
                        help='Temporary directory to use. By default, generate a new and unique one each time.')
    parser.add_argument('--qsub', default='qsub',
                        help='The qsub command. Replace by sh to test on your local machine only.')

    # cmd = sh.bash()

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
        try:
            if args.tmpdir == None:
                tmpdir = tempfile.mkdtemp()
            else:
                tmpdir = args.tmpdir
                if not os.path.exists(tmpdir):
                    os.makedirs(tmpdir)

            dump_in_n = J(tmpdir, 'dump_in')
            python_file_n = J(tmpdir, 'script.py')
            bash_file_n = J(tmpdir, 'script.sh')
            dump_out_n = J(tmpdir, 'dump_out')
            logfile = J(tmpdir, 'log')
            donefile = J(tmpdir, 'isdone')

            # generate the scripts
            qsub_script = gen_qsub_script(python_file_n, pre=args.pre, post=args.post,
                                          logfile=logfile, isdonefile=donefile)

            python_script = gen_python_script(
                pickleLoad.substitute(dump=dump_in_n),
                cell,
                genPickleDump(dumpfile=dump_out_n,
                              outvar=args.out))
            if not args.dry:
                # print(python_file, bash_file)
                # write the python and bash files
                with open(python_file_n, 'w') as python_file:
                    python_file.write(python_script)

                with open(bash_file_n, 'w') as bash_file:
                    bash_file.write(qsub_script)

                # dump the input variables
                with open(dump_in_n, 'wb') as dump_in:
                    pickle.dump(newNs, dump_in)

                # use fifo to wait for answer
                os.mkfifo(donefile)

                # execute process
                p = sh.Command(args.qsub)(bash_file_n, _bg=True)

                # wait for the task to complete
                with open(donefile, 'r') as donefifo:
                    r = donefifo.read()
                try:
                    exit_status = int(r.split('\n')[0])
                except:
                    exit_status = r

                if exit_status != 0:
                    raise Exception('An exception occured, got exit status', exit_status)

                if args.out != '':
                    with open(dump_out_n, 'rb') as dump_out:
                        # load the result
                        res = pickle.load(dump_out)

                        # import into local namespace using outname
                        self.ip.user_ns[args.out] = res
            else:
                print('Bash script')
                print('-----------')
                print(qsub_script)
                print()
                print('Python script')
                print('-------------')
                print(python_script)
                print()
                print('Temporary directory:', tmpdir)
                print(sh.ls(tmpdir))
        finally:
            if not args.noclean:
                sh.rm(tmpdir, r=True, f=True)

        return

    qsub.__doc__ = parser.format_help()


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(QsubMagics)

load_ipython_extension(get_ipython())
