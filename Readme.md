Spark Streaming

Task 1

The script extracts collocations from a corpus of texts (Wikipedia articles). A collocation is a pair of words that occur together more often than expected by chance, e.g. "references_external" or "Roman Empire."

The script utilizes the NPMI (Normalized Pointwise Mutual Information) metric for collocation extraction.

Workflow:

The PMI (Pointwise Mutual Information) of two words, a and b, is defined as:

PMI(a,b) = ln(P(a,b) / P(a)∗P(b)),
where P(a,b) is the probability of the two words occurring together, and  P(a) and P(b) are the probabilities of words a and b, respectively.
Higher PMI values indicate rarer collocations.

P(a) = num_of_occurrences_of_word_"a" / num_of_occurrences_of_all_words

P(ab) = num_of_occurrences_of_pair_"ab" / num_of_occurrences_of_all_pairs


NPMI calculation:

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

Condition:

Segments are defined based on user features logged in web service data. Each line in the input represents a user_id followed by user_agent information.

Segments:

Users browsing from iPhones.
Users using the Firefox browser.
Users using Windows.
Segments may overlap, and users can belong to multiple segments.

Heuristics:

Segment	Heuristic
seg_iphone	parsed_ua['device']['family'] like '%iPhone%'
seg_firefox	parsed_ua['user_agent']['family'] like '%Firefox%'
seg_windows	parsed_ua['os']['family'] like '%Windows%'
Estimate the number of unique users in each segment using the HyperLogLog algorithm (with an error rate of 1%). Output segments and user counts in the format: segment_name <tab> count, sorted by user count in descending order.

Some users can not fall into any of the specified segments, as real-life data often includes instances that are challenging to classify. Such users are simply excluded from the sample.

Additionally, segments may overlap (since it is possible that a user is using Windows with Firefox installed). To identify segments, the following heuristics can be employed (or alternative ones can be devised):
- Segment
- Heuristic

seg_iphone
parsed_ua['device']['family'] like '%iPhone%'

seg_firefox
parsed_ua['user_agent']['family'] like '%Firefox%'

seg_windows
parsed_ua['os']['family'] like '%Windows%'

Estimate the number of unique users in each segment using the HyperLogLog algorithm (with an error rate of 1%).

As a result, output the segments and the respective user counts in the following format: segment_name <tab> count. Sort the results based on the number of users in descending order.
