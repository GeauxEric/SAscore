#!/work/jaydy/anaconda2/envs/my-rdkit-env/bin/python

import luigi
import os
import sascorer
from rdkit import Chem


class Path(luigi.Task):

    task_name = luigi.Parameter()

    def getDataDir(self):
        return "/work/jaydy/dat/mb_sa"

    def getSmiPath(self):
        path = os.path.join(self.getDataDir(),
                            self.task_name)
        return path

    def getTopWorkDir(self):
        return "/work/jaydy/working/mb_sa"

    def getWorkDir(self):
        dirname = self.task_name.split('.')[0]
        path = os.path.join(self.getTopWorkDir(),
                            dirname)
        try:
            os.makedirs(path)
        except:
            pass
        return path


class CalculateSAscore(Path):

    def output(self):
        path = os.path.join(self.getWorkDir(),
                            self.task_name + ".sa.txt")
        return luigi.LocalTarget(path)

    def run(self):

        def runSA(ifn, ofn, error_ofn):
            err_ofs = open(error_ofn, 'w')
            with open(ofn, 'w') as ofs:
                with open(ifn, 'r') as ifs:
                    lines = ifs.readlines()
                    for line in lines:
                        try:
                            smi = line.split()[0]
                            m = Chem.MolFromSmiles(smi)
                            sa = sascorer.calculateScore(m)
                            ofs.write("%s\t%f\n" % (line.rstrip(), sa))
                        except Exception as inst:
                            err_ofs.write("%s\t%s\n" % (smi, inst.args))
            err_ofs.close()

        ifn = Path(self.task_name).getSmiPath()
        ofn = self.output().path
        error_ofn = ofn + ".err"
        runSA(ifn, ofn, error_ofn)


def main(task_name):
    luigi.build([
        CalculateSAscore(task_name),
    ],
                local_scheduler=True)
    pass


if __name__ == '__main__':
    import sys
    main(sys.argv[1])
