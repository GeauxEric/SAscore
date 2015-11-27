#!/work/jaydy/anaconda2/envs/my-rdkit-env/bin/python

import luigi
import os
import glob
import subprocess32
import sascorer
from rdkit import Chem


class Path(luigi.Task):
    task_name = luigi.Parameter()

    def getRootWorkDir(self):
        return "/work/jaydy/working/edud_sa"

    def getDataDir(self):
        return "/work/jaydy/dat/tars"

    def getTarPath(self):
        return os.path.join(self.getDataDir(),
                            self.task_name + '.tar.gz')

    def getWorkDir(self):
        path = os.path.join(self.getRootWorkDir(), self.task_name)
        try:
            os.makedirs(path)
        except:
            pass
        return path

    def getUntarredPath(self):
        path = os.path.join(self.getRootWorkDir(),
                            self.task_name)
        return path


class UnTar(Path):
    def requires(self):
        pass

    def output(self):
        path = self.getUntarredPath()
        return luigi.LocalTarget(path)

    def run(self):
        cmds = ['tar', 'xf', self.getTarPath(),
                '-C', self.getRootWorkDir()]
        subprocess32.call(cmds)


class CalculateSAscore(UnTar):

    def getAllSmiFiles(self):
        regx = os.path.join(self.getUntarredPath(),
                            "*/*.sdf-all.smi")
        files = glob.glob(regx)
        return files

    def requires(self):
        return UnTar(self.task_name)

    def output(self):
        smi_fns = self.getAllSmiFiles()
        outputs = []
        for smi_fn in smi_fns:
            out_fn = smi_fn + '.sa.txt'
            outputs.append(luigi.LocalTarget(out_fn))
        return outputs

    def runSA(self, ifn, ofn, error_ofn):
        err_ofs = open(error_ofn, 'w')
        with open(ofn, 'w') as ofs:
            with open(ifn, 'r') as ifs:
                smis = [_.rstrip() for _ in ifs]
                for smi in smis:
                    try:
                        m = Chem.MolFromSmiles(smi)
                        sa = sascorer.calculateScore(m)
                        ofs.write("%s\t%f\n" % (smi, sa))
                    except Exception as inst:
                        err_ofs.write("%s\t%s\n" % (smi, inst.args))
        err_ofs.close()

    def run(self):
        smi_fns = self.getAllSmiFiles()
        for smi_fn in smi_fns:
            out_fn = smi_fn + '.sa.txt'
            error_ofn = smi_fn + '.sa.error'
            self.runSA(smi_fn, out_fn, error_ofn)


def main(task_name):
    luigi.build([
        CalculateSAscore(task_name),
    ],
                local_scheduler=True)
    pass


if __name__ == '__main__':
    import sys
    main(sys.argv[1])
