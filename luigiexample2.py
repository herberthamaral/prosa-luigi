import datetime
import requests

import luigi
import tika.parser

from luigi.contrib.postgres import PostgresTarget

URL_BASE = 'https://dje.tjsp.jus.br/cdje/downloadCaderno.do?dtDiario={data_diario}&cdCaderno={caderno}'
CADERNO_CHOICES = [10, 11, 12, 13, 14, 15, 18]


class BaixaJornalTJSP(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(1))
    caderno = luigi.ChoiceParameter(choices=CADERNO_CHOICES, var_type=int)

    def run(self):
        data_diario = self.date.strftime('%d/%m/%Y')
        url = URL_BASE.format(data_diario=data_diario, caderno=str(self.caderno))
        caderno = requests.get(url, verify=False)
        if caderno.status_code == 200:
            with self.output().open('wb') as output:
                output.write(caderno.content)

    def output(self):
        return luigi.LocalTarget(
            f'/tmp/tjsp/caderno-{self.date.isoformat()}-{self.caderno}.pdf',
            format=luigi.format.Nop,
        )


class ConverteParaTexto(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(1))
    caderno = luigi.ChoiceParameter(choices=CADERNO_CHOICES, var_type=int)

    def requires(self):
        return BaixaJornalTJSP(self.date, self.caderno)

    def run(self):
        parsed = tika.parser.from_file(self.input().path)
        if parsed['status'] == 200:
            with self.output().open('w') as output:
                output.write(parsed['content'])
        else:
            raise RuntimeError('Deu ruim no tika')

    def output(self):
        return luigi.LocalTarget(f'/tmp/tjsp/parsed-{self.date.isoformat()}-{self.caderno}.txt')


class ConstroiIndiceWhoosh(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(1))
    caderno = luigi.ChoiceParameter(choices=CADERNO_CHOICES, var_type=int)

    def requires(self):
        return ConverteParaTexto(self.date, self.caderno)

    def run(self):
        self.output().touch()

    def output(self):
        return PostgresTarget(
            host='localhost',
            database='luigi',
            user='postgres',
            password='',
            table='constroi-indice-whoosh',
            update_id=self.task_id,
        )


class ExtraiNotas(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(1))
    caderno = luigi.ChoiceParameter(choices=CADERNO_CHOICES, var_type=int)

    def requires(self):
        return ConverteParaTexto(self.date, self.caderno)

    def run(self):
        self.output().touch()

    def output(self):
        return PostgresTarget(
            host='localhost',
            database='luigi',
            user='postgres',
            password='',
            table='extrai-notas',
            update_id=self.task_id,
        )


class ProcessamentoWrapper(luigi.Task):

    date_start = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(31))
    date_end = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(2))

    def requires(self):
        tasks = []
        while self.date_start <= self.date_end:
            for jornal in CADERNO_CHOICES:
                tasks.append(ConstroiIndiceWhoosh(self.date_start, jornal))
                tasks.append(ExtraiNotas(self.date_start, jornal))
                self.date_start += datetime.timedelta(1)
        return tasks


if __name__ == '__main__':
    luigi.build([ProcessamentoWrapper()], workers=5)  # agora usando o central-scheduler
