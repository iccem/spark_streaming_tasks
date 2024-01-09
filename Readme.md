Spark Streaming

Task 1

The script extracts collocations from a corpus of texts (Wikipedia articles). A collocation is a pair of words that occur together more often than expected by chance, such as "references_external" or "Roman Empire."

The script utilizes the NPMI (Normalized Pointwise Mutual Information) metric for collocation extraction.

Workflow:

The PMI (Pointwise Mutual Information) of two words, a and b, is defined as:

PMI(a,b) = ln(P(a,b) / P(a)∗P(b)),
Where P(a,b) is the probability of the two words occurring together, and  P(a) and P(b) are the probabilities of words a and b, respectively.
Higher PMI values indicate rarer collocations.

P(a) = num_of_occurrences_of_word_"a" / num_of_occurrences_of_all_words

P(ab) = num_of_occurrences_of_pair_"ab" / num_of_occurrences_of_all_pairs

total_number_of_words - общее кол-во слов в тексте
total_number_of_word_pairs - общее кол-во пар

Расчет NPMI:

NPMI(a,b) = −(PMI(a,b) / lnP(a,b))

Normalization of NPMI to the range [-1, 1].

Data Processing:

All non-Latin characters are discarded during parsing.
All words are converted to lowercase.
"Stop words" are removed, including those within bigrams (e.g., "at evening" is treated the same as "at the evening").
Bigrams are joined by an underscore "_" in the output.
NPMI is calculated only for bigrams that occur more than 500 times.
Total counts are computed before filtering.
The top 39 most popular collocations, sorted by descending NPMI values, are displayed on STDOUT.



Task 2

Spark Streaming

Input Data: /data/realtime/uids

Data Format:
...
seg_firefox 4176
...

Условие
Сегмент - это множество пользователей, определяющееся неким признаком. Когда пользователь посещает web-сервис со своего устройства, это событие логируется на стороне web-сервиса в следующем формате: user_id <tab> user_agent. Например:

f78366c2cbed009e1febc060b832dbe4	Mozilla/5.0 (Linux; Android 4.4.2; T1-701u Build/HuaweiMediaPad) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.73 Safari/537.36
62af689829bd5def3d4ca35b10127bc5	Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36


На вход поступают порции web-логов в описанном формате. Требуется разбить аудиторию (пользователей) в этих логах на следующие сегменты:

Пользователи, которые работают в интернете из-под IPhone.
Пользователи, кот. используют Firefox браузер.
Пользователи, кот. используют Windows.

Не стоит волноваться если какие-то пользователи не попадут ни в 1 из указанных сегментов поскольку в реальной жизни часто попадаются данные, которые сложно классифицировать. Таких пользователей просто не включаем в выборку.
Также сегменты могут пересекаться (ведь возможен вариант, что пользователь использует Windows, на котором стоит Firefox). Для того, чтоб выделить сегменты можно использовать следующие эвристики (или придумать свои):



Сегмент
Эвристика




seg_iphone
parsed_ua['device']['family'] like '%iPhone%'


seg_firefox
parsed_ua['user_agent']['family'] like '%Firefox%'


seg_windows
parsed_ua['os']['family'] like '%Windows%'



Оцените кол-во уникальных пользователей в каждом сегменте используя алгоритм 
HyperLogLog (error_rate равным 1%).

В результате выведите сегменты и количества пользователей в следующем формате: segment_name <tab> count. Отсортируйте результат по количеству пользователей в порядке убывания.
