import luigi


class MyTask(luigi.Task):

    parameter = luigi.Parameter(default='xablau')

    def run(self):
        with self.output().open('w') as output:
            output.write('resultado de algum processamento')

    def output(self):
        return luigi.LocalTarget(f'/tmp/{self.parameter}')


if __name__ == '__main__':
    luigi.run()
