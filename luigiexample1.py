import datetime
import requests
import luigi
import tika.parser

URL_BASE = 'https://dje.tjsp.jus.br/cdje/downloadCaderno.do?dtDiario={data_diario}&cdCaderno={caderno}'
CADERNO_CHOICES = [10, 11, 12, 13, 14, 15, 18]


class BaixaJornalTJSP(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(2))
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

    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(2))
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


if __name__ == '__main__':
    tasks = [
        ConverteParaTexto(caderno=caderno)
        for caderno in CADERNO_CHOICES
    ]
    luigi.build(tasks, local_scheduler=True, workers=5)
