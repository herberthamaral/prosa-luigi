Prosa do luigi
==============


O [luigi](https://luigi.readthedocs.io/en/stable/) é um pacote do Python que
permite a criação e execução de pipelines (DAGs - grafos direcionados e
acíclicos, em inglês), muito comum em tarefas de
[ETL](https://pt.wikipedia.org/wiki/Extract,_transform,_load)
(extract,transform and load).

A apresentação está disponível no [Google Drive](https://docs.google.com/presentation/d/1Rf0illRDiw4Ls0XzxqBwwqerG7JbZQ7oLypcjlSjb-0/edit#slide=id.p).


Executando
----------

Você vai precisar do Python 3.6 (versões diferentes podem funcionar, mas não
foram testadas) e do pipenv instalados. Se você quiser executar o
`luigiexample2.py`, você vai precisar do PostgreSQL instalado e configurado
(verifique o arquivo para informações de conexão). Inicie o ambiente da
seguinte forma:

    git clone https://github.com/herberthamaral/prosa-luigi
    cd prosa-luigi
    pipenv install
    pipenv shell


Para execcutar o exemplo 0 e 1, apenas chame o script pela linha de comando:

    python luigiexample0.py
    python luigiexample1.py

Para executar o `luigiexample2.py`, inicie o `luigid` (scheduler do luigi) antes:

    luigid


Para executar pela linha de comando diretamente, siga o exemplo:

    PYTHONPATH=. luigi --module=luigiexample1 ConverteParaTexto --date=2018-12-04


Observação
==========

As tarefas podem falhar por causa das férias forenses: estamos baixando PDFs do
diário oficial do TJSP. Talvez você queira acertar as datas de download (veja o
último exemplo da seção anterior).



Licença: MIT
