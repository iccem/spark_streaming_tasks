Spark Streaming

Task 1
Скрипт извлекает коллокации из корпуса текстов (статьи Википедии). 
Коллокация - два последовательно стоящих слова (пара слов), которые чаще других встречаются вместе. Например, «references_external» или «Roman Empire».

Для поиска коллокаций используется метрика NPMI (нормализованная точечная взаимная информация).


Порядок работы

PMI двух слов a и b определяется как:

PMI(a,b) = ln(P(a,b) / P(a)∗P(b)),

где P(ab) - вероятность двух слов, идущих подряд, а P(a) и P (b) - вероятности слов a и b соответственно.
Чем больше значение PMI, тем реже встречается коллокация.


Оцениваем вероятности встречаемости слов:
число вхождений каждого слова разделим на общее число слов в корпусе

P(a) = num_of_occurrences_of_word_"a" / num_of_occurrences_of_all_words

P(ab) = num_of_occurrences_of_pair_"ab" / num_of_occurrences_of_all_pairs

total_number_of_words - общее кол-во слов в тексте
total_number_of_word_pairs - общее кол-во пар

Расчет NPMI:

NPMI(a,b) = −(PMI(a,b) / lnP(a,b))

Нормализуем величину в диапазон [-1; 1].


Обработка данных

При парсинге отбрасываются все символы, которые не являются латинскими буквами.
Все слова приводятся к нижнему регистру.
Удаляются "стоп-слова", в том числе внутри биграмм, например, “at evening” имеет ту же семантику что и “at the evening”, список "слоп-слов" прилагается.
Биграммы при выводе объединяются символом нижнего подчеркивания "_".
Считается NPMI только для биграмм, которые встретились чаще 500 раз.
Общее число слов и биграмм считаются до фильтрации.

На экран выводится (в STDOUT) TOP-39 самых популярных коллокаций, отсортированных по убыванию значения NPMI. Само значение NPMI  не выводится.