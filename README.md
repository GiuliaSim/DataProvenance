# DataProvenance
Questo repository contiene il codice del progetto di tesi relativo alla data provenance nell'ambito della pre-elaborazione dei dati nella data science: *Capturing and querying fine-grained provenance of preprocessing pipelines in data science*.

Il codice è scritto in [Python](https://www.python.org/)

## Organizzazione del repository
Questo repository è organizzato in due sezioni (cartelle) principali:
1. [prov_acquisition](prov_acquisition/) contiene la parte relativa all'acquisizione della provenance dei dati. 
 Questa cartella è suddivisa in altre tre sezioni:
    * [prov_libraries](prov_acquisition/prov_libraries/) - le due implementazioni per l'acquisizione della data provenance. 
    * [real_world_pipeline](prov_acquisition/real_world_pipeline/) - acquisizione della provenienza dei dati su tre pipeline reali che coinvolgono diversi tipi di fasi di pre-elaborazione.
    * [preprocessing_methods](prov_acquisition/preprocessing_methods/) - esempi di acquisizione della provenienza per ogni operazione di pre-elaborazione implementata su dataset generati da *DIGen*, il generatore di dati fornito dal [TPC](http://www.tpc.org/tpcdi/).
2. [queries](queries/) contiene il codice per l'interrogazione della data provenance.

## Librerie necessarie per eseguire il codice

* [PROV Python library](https://pypi.org/project/prov/): un'implementazione del [Provenance Data Model](https://www.w3.org/TR/prov-dm/) del World Wide Web Consortium.
* [pymongo](https://pymongo.readthedocs.io/en/stable/)
* [pandas](https://pandas.pydata.org/)

## Esecuzione

### Acquisizione
1. Per eseguire gli esempi sulle tre pipeline reali:
    * Posizionarsi nella cartella *prov_acquisition/*
    * Eseguire il file con il comando `python real_world_pipeline/*.py` seguito da `-op` se si desidera utilizzare l'implementazione ottimizata.
    Ad esempio, **python real_world_pipeline/GermanCleanup_prov.py -op**
    
    
Il risultato verrà memorizzato nella cartella *prov_acquisition/prov_results/<nome_dataset>*.
Ad esempio, **prov_acquisition/prov_results/German**

2. Per gli esempi che usano i dataset generati dal DIGen:
    * Posizionarsi nella cartella *prov_acquisition/*
    * Eseguire il file con il comando `python preprocessing_methods/*.py -i <input_file>` seguito da `-op` se si desidera utilizzare l'implementazione ottimizata.
    Ad esempio, **python preprocessing_methods/FT_prov.py -i datasets/Trade_SF3.csv -op**
    
Anche in questo caso il risultato verrà memorizzato nella cartella *prov_acquisition/prov_results/<nome_dataset>*.
Ad esempio, **prov_acquisition/prov_results/Trade_SF3**


### Interrogazione
