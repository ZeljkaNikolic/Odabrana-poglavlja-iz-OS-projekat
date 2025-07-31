Projekat iz predmeta Odabrana poglavlja iz Operativnih sistema (2024/25)

~ Analiza Spotify skupa podataka ~

* Data stavka projektnog zadatka će obuhvatiti analizu Spotify skupa podataka koristeći Spark
DataFrame API. Za realizaciju datog zadatka potrebno je prvo definisati tačnu šemu podataka koja koristi
adekvatne tipove na osnovu opisa podataka. Validirati da se podaci adekvatno učitavaju. Za svaku učitanu
kolonu potrebno je izvršiti analizu distribucije podataka koju je zatim potrebno sačuvati u JSON formatu.

* Identifikovati 10 najčešćih kolaboracija između umjetnika (dva ili više umjetnika koji nastupaju zajedno
na pjesmi) i izračunajte prosječnu popularnost njihovih pesama. Za prosječnu razliku popularnosti solo
pjesama manje popularnog izvođača i prosječne popularnosti kolaboracije.

* Pronađite “breakthrough” pjesme (popularnost >80 u albumima u prosjeku <50) i analizirajte razliku
njihovih audio karakteristika (energija, plesnost, valencija) u odnosu na prosječne karakteristike u ostalim
pjesmama unutar albuma.

* Za 5 najpopularnijih žanrova, utvrdite da li postoje optimalni rasponi tempa "sweet spot" upoređujući
prosječnu popularnost pjesama unutar različitih tempa (npr. <100 BPM, 100-120 BPM, >120 BPM).

* Uporedite korelaciju između eksplicitnog sadržaja i popularnosti u različitim žanrovima kako biste
utvrdili u kojim je žanrovima eksplicitni sadržaj pozitivan ili negativan faktor za uspjeh pjesme.

* Za 10 najdužih pjesama sa visokom ocjenom plesivosti (>0,8), uporedite njihov komercijalni uspjeh
(popularnost) sa prosječnom popularnošću pjesama u istom žanru kako biste utvrdili da li duže trajanje
utiče na uspjeh.

* Analizirajte da li eksplicitne i neeksplicitne pjesme pokazuju različite obrasce u valentnosti (muzička
pozitivnost) i utvrdite da li je ovaj odnos dosljedan na svim razinama popularnosti (posmatrati opsege
popularnosti u veličini od 0.1)

* Za umjetnike s najdosljednijom popularnošću (najniža standardna devijacija), analizirajte njihovu
žanrovsku raznolikost kako biste utvrdili da li je dosljednost povezana sa specijalizacijom za određene
žanrove.

* U žanrovima sa visokim akustičnim i instrumentalnim vrijednostima (>0,8), uporedite njihovu
prosječnu popularnost sa vokalno teškim žanrovima kako biste utvrdili da li instrumentalni fokus utiče na
komercijalni uspjeh.

