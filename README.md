
# Crawler
Cieĺom projektu bol vytvoriť si databázu filmov. V 1. fáze som vytvoril vlastný crawler, ktorý pozbieral linky z imdb.com. 
Túto časť som vytvoril v pythone. Dáta získam z responsov url na ktoré sa posielajú requesty. Na začiatku programu vytvorím tabuľky.
Do jednej uložím všetky pozbierané url a responsy, a potom do druhej uložím len vyparsované dát filmov a seriálov.
Pred crawlovaním deklarujem set do ktorého uložím už navštívené url aby som zabezpečil, že neposielam request na jeden url 2x.
Okrem toho potrebujem jeden queue, do ktorého uložím nacrawlované url, ktoré ešte budem navštíviť. V každej iterácii pomocou regexu nájdem všetky linky  v aktuálnom response.
V tomto kroku bolo treba riešiť aj alternatívne linky, keďže majú iný formát. Nacrawlované linky uložím do queue a posielam request na ďalší url. 

V 2. časti parsujem informácie z dát ktoré som nacrawloval v prvej časti. Iteratívne prejdem celý dataframe responsov. 
Táto tabuľka obsahuje 874 MB dát, z ktorých bolo treba vyfiltrovať filmy. V každej iterácii kontrolujem či url je url stránky filmu/seriálu - obsahuje string title.
Bolo treba ušetriť aby restrictedWords neboli v url, lebo tie stránky neobsahovali informácie ktoré som hľadal. Keď url je správny, tak začína sa parsovaie pomocou regexu.
Výsledné informácie sú uložené do tabuľky. Parsované dáta sú: návoz filmu, režisér a cast. Tabuľka parsovaných filmov obsahovala 710 filmov.
Vo vypracovaní 1. časti som čerpal informácie z tohto článku:
Peshave, M., & Dezhgosha, K. (n.d.). HOW SEARCH ENGINES WORK AND A WEB CRAWLER APPLICATION.


# Parovanie s Wiki dáta
Druhé zadanie bolo prelinkovanie týchto dát pomocou sparku s informáciami z Wikipédii. Na začiatku bolo treba nainštalovať spark a hadoop.
Následne inicializoval som ich v kóde a načítal som dump súbor podľa spark schémy. Pracoval som so súbormi enwiki-latest-pages-articles.xml a enwiki-latest-pages-articles17.xml-p20570393p22070392.
Prešiel som celý dump, a načítal som všetko čo bolo medzi tagmi <page> a </page>, takto som uložil len kód wiki stránok. Následne som kontroloval,
či <revision> obsahuje string Infobox film. Takto som vyfiltroval stránky, ktoré boli o filmoch.

Pomocou udf funkcií a regexov som našiel dodatočné info. Keď nejaká informácia sa nenašla, tak potom som doplnil string ‘not found’.
Niektoré informácie bolo treba ešte prečistiť. Napr keď sa našli viac režisérov,
tak regex ich našiel ako jeden celý string. Bolo treba prepacovať a vytvoriť z ich list stringov. 

V poslednej časti som spoji tabuľky z imdb a z Wikipédii. Join sa vykonal na riadkoch kde názol filmu a režiséri boli také isté. 
Okrem toho môže sa stať, že film mal viac režisérov, v tomto prípade, ak aspoň jeden režisér sa našiel v liste režisérov v oboch tabuľkách, tak sa join vykonal. 
Alebo keď názvy boli také isté, ale jedna tabuľka neobsahovala žiadneho režiséra, teda mala hodnotu more.

# Precision Recall
Následne som vypočítal Precision Recall Accuracy s dvoma spôsobmi podľa nájdených dodatočných informácií.
## True Positives (TP):
  Kód kontroluje, či "director_list" a "director_wiki" nie sú NaN, a ak nie sú, porovná ich po konverzii na malé písmená.
  Výsledkom je počet riadkov, v ktorých sa riaditelia zhodujú.
## False Positives (FP): 
  Celkový počet riadkov mínus pravdivé pozitívne výsledky.
## False Negatives (FN):
  Kontroluje, či "title_wiki" a "title" nie sú NaN, a ak nie sú, kontroluje, či sa nezhodujú. Výsledkom je počet riadkov, v ktorých je zhoda vynechaná.

## Precision:
  Pomer pravdivých pozitívnych výsledkov k súčtu pravdivých a falošných pozitívnych výsledkov.
## Recall:
  Pomer Pravdivých pozitívnych výsledkov k súčtu Pravdivých pozitívnych výsledkov a Falošne negatívnych výsledkov.
## Precision: 0.47
## Recall: 0.49

Druhá metóda iteruje každý riadok v DataFrame pomocou iterrows() a kontroluje zhodu podobne ako metóda 1.
Zvyšuje počet True Positives, False Positives a False Negatives. Po ukončení cyklu vypočíta Precision a Recall pomocou rovnakých vzorcov ako v metóde 1.

Obe metódy sa zameriavajú na dosiahnutie rovnakého cieľa: výpočet Precision a Recall pre zhodné záznamy v spojenom DataFrame.
Metóda 1 využíva vektorové operácie, ktoré môžu byť efektívnejšie, najmä v prípade väčších DataFrame, zatiaľ čo metóda 2 využíva tradičnejší iteračný prístup.
## Precision: 0.63
## Recall: 0.65

# TESTS
Vytvoril som 5 testov na vyhladávanie, ktoré sú v súbore indexer.py
Scenáre: jednoduchý search, case insesivity search, partial string search, release search, search of empty ( not found ) values

